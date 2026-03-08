#include "ruby.h"
#include "ruby/thread.h"
#include <dlfcn.h>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    const char *source_host;
    uint16_t source_port;
    const char *source_user;
    const char *source_password;
    const char *source_database;
    const char *source_socket_path;
    uint8_t source_tls_mode;
    const char *slot_name;
    const char *publication_names;
    const char *proto_version;
    const char *dest_host;
    uint16_t dest_port;
    const char *dest_user;
    const char *dest_password;
    const char *dest_database;
    const char *dest_socket_path;
    uint8_t dest_tls_mode;
    const char *source_id;
    uint32_t max_batch_size;
    void (*on_flush)(void *context, uint64_t start_lsn, uint64_t end_lsn, size_t msg_count, bool is_complete);
    void *on_flush_context;
} pgzr_ingest_config_t;

typedef struct {
    const char *dest_host;
    uint16_t dest_port;
    const char *dest_user;
    const char *dest_password;
    const char *dest_database;
    const char *dest_socket_path;
    uint8_t dest_tls_mode;
    const char *source_id;
    uint32_t poll_interval_ms;
    const char *metadata_message_prefix;
    const char *metadata_table;
} pgzr_processor_config_t;

typedef const char *(*pgzr_last_error_fn)(size_t *out_len);
typedef void *(*pgzr_ingestor_new_fn)(const pgzr_ingest_config_t *config);
typedef int (*pgzr_ingestor_run_fn)(void *ingestor);
typedef void (*pgzr_ingestor_stop_fn)(void *ingestor);
typedef void (*pgzr_ingestor_free_fn)(void *ingestor);
typedef void *(*pgzr_processor_new_fn)(const pgzr_processor_config_t *config);
typedef int (*pgzr_processor_run_fn)(void *processor);
typedef int (*pgzr_processor_process_one_fn)(void *processor);
typedef void (*pgzr_processor_stop_fn)(void *processor);
typedef void (*pgzr_processor_free_fn)(void *processor);

typedef struct {
    void *handle;
    pgzr_last_error_fn last_error;
    pgzr_ingestor_new_fn ingestor_new;
    pgzr_ingestor_run_fn ingestor_run;
    pgzr_ingestor_stop_fn ingestor_stop;
    pgzr_ingestor_free_fn ingestor_free;
    pgzr_processor_new_fn processor_new;
    pgzr_processor_run_fn processor_run;
    pgzr_processor_process_one_fn processor_process_one;
    pgzr_processor_stop_fn processor_stop;
    pgzr_processor_free_fn processor_free;
} pgzr_library_t;

typedef struct {
    char **items;
    size_t length;
    size_t capacity;
} pgzr_string_store_t;

typedef struct {
    void *ptr;
    pgzr_ingest_config_t config;
    pgzr_string_store_t strings;
    VALUE on_flush;
    VALUE callback_error;
    bool running;
} pgzr_ingestor_wrapper_t;

typedef struct {
    void *ptr;
    pgzr_processor_config_t config;
    pgzr_string_store_t strings;
    bool running;
} pgzr_processor_wrapper_t;

typedef struct {
    pgzr_ingestor_wrapper_t *wrapper;
    uint64_t start_lsn;
    uint64_t end_lsn;
    size_t msg_count;
    bool is_complete;
} pgzr_flush_call_t;

typedef struct {
    pgzr_ingestor_wrapper_t *wrapper;
    int rc;
} pgzr_ingestor_run_call_t;

typedef struct {
    pgzr_processor_wrapper_t *wrapper;
    int rc;
} pgzr_processor_run_call_t;

typedef struct {
    pgzr_processor_wrapper_t *wrapper;
    int rc;
} pgzr_processor_process_one_call_t;

static VALUE mPGZR;
static VALUE cIngestor;
static VALUE cProcessor;
static ID id_call;
static ID id_host;
static ID id_port;
static ID id_user;
static ID id_password;
static ID id_database;
static ID id_socket_path;
static ID id_tls_mode;
static ID id_source;
static ID id_dest;
static ID id_source_id;
static ID id_max_batch_size;
static ID id_on_flush;
static ID id_poll_interval_ms;
static ID id_metadata_message_prefix;
static ID id_metadata_table;
static ID id_slot_name;
static ID id_publication_names;
static ID id_proto_version;

static pgzr_library_t pgzr_lib = {0};

static VALUE pgzr_error_class(void) {
    return rb_path2class("PGZR::Error");
}

static void pgzr_string_store_append(pgzr_string_store_t *store, char *ptr) {
    if (store->length == store->capacity) {
        size_t next_capacity = store->capacity == 0 ? 8 : store->capacity * 2;
        REALLOC_N(store->items, char *, next_capacity);
        store->capacity = next_capacity;
    }

    store->items[store->length++] = ptr;
}

static void pgzr_string_store_free(pgzr_string_store_t *store) {
    size_t i;

    if (store->items != NULL) {
        for (i = 0; i < store->length; i++) {
            xfree(store->items[i]);
        }
        xfree(store->items);
    }

    store->items = NULL;
    store->length = 0;
    store->capacity = 0;
}

static char *pgzr_dup_c_string(VALUE value) {
    VALUE string = rb_obj_as_string(value);
    const char *source = StringValueCStr(string);
    long length = RSTRING_LEN(string);
    char *copy = ALLOC_N(char, (size_t)length + 1);

    memcpy(copy, source, (size_t)length + 1);
    return copy;
}

static char *pgzr_store_string(pgzr_string_store_t *store, VALUE value) {
    char *copy;

    if (NIL_P(value)) {
        return NULL;
    }

    copy = pgzr_dup_c_string(value);
    pgzr_string_store_append(store, copy);
    return copy;
}

static VALUE pgzr_hash_lookup_symbol(VALUE hash, ID key_id) {
    return rb_hash_lookup2(hash, ID2SYM(key_id), Qnil);
}

static VALUE pgzr_required_keyword(VALUE hash, ID key_id, const char *name) {
    VALUE value = pgzr_hash_lookup_symbol(hash, key_id);

    if (NIL_P(value)) {
        rb_raise(rb_eArgError, "missing keyword: %s", name);
    }

    return value;
}

static uint8_t pgzr_tls_mode_value(VALUE mode) {
    ID mode_id;

    if (NIL_P(mode)) {
        return 0;
    }

    mode_id = SYM2ID(rb_to_symbol(mode));

    if (mode_id == rb_intern("disable")) return 0;
    if (mode_id == rb_intern("prefer")) return 1;
    if (mode_id == rb_intern("require")) return 2;
    if (mode_id == rb_intern("verify_full")) return 3;

    rb_raise(rb_eArgError, "unknown TLS mode: %" PRIsVALUE " (expected :disable, :prefer, :require, or :verify_full)", rb_inspect(mode));
    UNREACHABLE_RETURN(0);
}

static uint16_t pgzr_uint16_value(VALUE value, const char *name) {
    unsigned long n;

    if (NIL_P(value)) {
        return 0;
    }

    n = NUM2ULONG(value);
    if (n > USHRT_MAX) {
        rb_raise(rb_eRangeError, "%s must be between 0 and %u", name, USHRT_MAX);
    }

    return (uint16_t)n;
}

static uint32_t pgzr_uint32_value(VALUE value, const char *name) {
    unsigned long n;

    if (NIL_P(value)) {
        return 0;
    }

    n = NUM2ULONG(value);
    if (n > UINT32_MAX) {
        rb_raise(rb_eRangeError, "%s must be between 0 and %u", name, UINT32_MAX);
    }

    return (uint32_t)n;
}

static void *pgzr_resolve_symbol(const char *name) {
    void *symbol;
    const char *error;

    dlerror();
    symbol = dlsym(pgzr_lib.handle, name);
    error = dlerror();
    if (symbol == NULL || error != NULL) {
        rb_raise(rb_eLoadError, "failed to resolve %s from libpgzr: %s", name, error ? error : "unknown error");
    }

    return symbol;
}

static void pgzr_load_library(void) {
    const char *env_path;

    if (pgzr_lib.handle != NULL) {
        return;
    }

    env_path = getenv("PGZR_LIB_PATH");
    if (env_path != NULL && env_path[0] != '\0') {
        pgzr_lib.handle = dlopen(env_path, RTLD_NOW | RTLD_LOCAL);
        if (pgzr_lib.handle == NULL) {
            rb_raise(rb_eLoadError, "failed to load libpgzr from PGZR_LIB_PATH=%s: %s", env_path, dlerror());
        }
    } else {
#ifdef __APPLE__
        const char *candidates[] = {"libpgzr.dylib", "libpgzr.0.dylib", NULL};
#else
        const char *candidates[] = {"libpgzr.so", "libpgzr.so.0", NULL};
#endif
        int i;

        for (i = 0; candidates[i] != NULL; i++) {
            pgzr_lib.handle = dlopen(candidates[i], RTLD_NOW | RTLD_LOCAL);
            if (pgzr_lib.handle != NULL) {
                break;
            }
        }

        if (pgzr_lib.handle == NULL) {
            rb_raise(rb_eLoadError, "failed to load libpgzr; set PGZR_LIB_PATH to the full shared library path");
        }
    }

    pgzr_lib.last_error = (pgzr_last_error_fn)pgzr_resolve_symbol("pgzr_last_error");
    pgzr_lib.ingestor_new = (pgzr_ingestor_new_fn)pgzr_resolve_symbol("pgzr_ingestor_new");
    pgzr_lib.ingestor_run = (pgzr_ingestor_run_fn)pgzr_resolve_symbol("pgzr_ingestor_run");
    pgzr_lib.ingestor_stop = (pgzr_ingestor_stop_fn)pgzr_resolve_symbol("pgzr_ingestor_stop");
    pgzr_lib.ingestor_free = (pgzr_ingestor_free_fn)pgzr_resolve_symbol("pgzr_ingestor_free");
    pgzr_lib.processor_new = (pgzr_processor_new_fn)pgzr_resolve_symbol("pgzr_processor_new");
    pgzr_lib.processor_run = (pgzr_processor_run_fn)pgzr_resolve_symbol("pgzr_processor_run");
    pgzr_lib.processor_process_one = (pgzr_processor_process_one_fn)pgzr_resolve_symbol("pgzr_processor_process_one");
    pgzr_lib.processor_stop = (pgzr_processor_stop_fn)pgzr_resolve_symbol("pgzr_processor_stop");
    pgzr_lib.processor_free = (pgzr_processor_free_fn)pgzr_resolve_symbol("pgzr_processor_free");
}

static VALUE pgzr_last_error_value(void) {
    const char *message;
    size_t length = 0;

    pgzr_load_library();
    message = pgzr_lib.last_error(&length);
    if (message == NULL || length == 0) {
        return Qnil;
    }

    return rb_utf8_str_new(message, (long)length);
}

static VALUE pgzr_last_error_method(VALUE self) {
    (void)self;
    return pgzr_last_error_value();
}

static VALUE pgzr_tls_mode_value_method(VALUE self, VALUE mode) {
    (void)self;
    return UINT2NUM(pgzr_tls_mode_value(mode));
}

static void pgzr_raise_last_error(const char *fallback) {
    VALUE message = pgzr_last_error_value();

    if (NIL_P(message)) {
        rb_raise(pgzr_error_class(), "%s", fallback);
    }

    rb_exc_raise(rb_exc_new_str(pgzr_error_class(), message));
}

static void pgzr_set_conn_fields(
    pgzr_string_store_t *store,
    VALUE hash,
    const char *port_name,
    const char **host,
    uint16_t *port,
    const char **user,
    const char **password,
    const char **database,
    const char **socket_path,
    uint8_t *tls_mode
) {
    VALUE value;

    Check_Type(hash, T_HASH);

    value = pgzr_hash_lookup_symbol(hash, id_host);
    *host = pgzr_store_string(store, value);

    value = pgzr_hash_lookup_symbol(hash, id_port);
    *port = pgzr_uint16_value(value, port_name);

    value = pgzr_hash_lookup_symbol(hash, id_user);
    *user = pgzr_store_string(store, value);

    value = pgzr_hash_lookup_symbol(hash, id_password);
    *password = pgzr_store_string(store, value);

    value = pgzr_hash_lookup_symbol(hash, id_database);
    *database = pgzr_store_string(store, value);

    value = pgzr_hash_lookup_symbol(hash, id_socket_path);
    *socket_path = pgzr_store_string(store, value);

    value = pgzr_hash_lookup_symbol(hash, id_tls_mode);
    *tls_mode = pgzr_tls_mode_value(value);
}

static void pgzr_ingestor_mark(void *ptr) {
    pgzr_ingestor_wrapper_t *wrapper = (pgzr_ingestor_wrapper_t *)ptr;

    rb_gc_mark(wrapper->on_flush);
    rb_gc_mark(wrapper->callback_error);
}

static void pgzr_ingestor_free_data(void *ptr) {
    pgzr_ingestor_wrapper_t *wrapper = (pgzr_ingestor_wrapper_t *)ptr;

    if (wrapper->ptr != NULL && pgzr_lib.ingestor_free != NULL) {
        pgzr_lib.ingestor_free(wrapper->ptr);
        wrapper->ptr = NULL;
    }

    pgzr_string_store_free(&wrapper->strings);
    xfree(wrapper);
}

static size_t pgzr_ingestor_memsize(const void *ptr) {
    return ptr == NULL ? 0 : sizeof(pgzr_ingestor_wrapper_t);
}

static const rb_data_type_t pgzr_ingestor_type = {
    "PGZR::Ingestor",
    {
        pgzr_ingestor_mark,
        pgzr_ingestor_free_data,
        pgzr_ingestor_memsize,
    },
    0,
    0,
    RUBY_TYPED_FREE_IMMEDIATELY,
};

static void pgzr_processor_mark(void *ptr) {
    (void)ptr;
}

static void pgzr_processor_free_data(void *ptr) {
    pgzr_processor_wrapper_t *wrapper = (pgzr_processor_wrapper_t *)ptr;

    if (wrapper->ptr != NULL && pgzr_lib.processor_free != NULL) {
        pgzr_lib.processor_free(wrapper->ptr);
        wrapper->ptr = NULL;
    }

    pgzr_string_store_free(&wrapper->strings);
    xfree(wrapper);
}

static size_t pgzr_processor_memsize(const void *ptr) {
    return ptr == NULL ? 0 : sizeof(pgzr_processor_wrapper_t);
}

static const rb_data_type_t pgzr_processor_type = {
    "PGZR::Processor",
    {
        pgzr_processor_mark,
        pgzr_processor_free_data,
        pgzr_processor_memsize,
    },
    0,
    0,
    RUBY_TYPED_FREE_IMMEDIATELY,
};

static VALUE pgzr_ingestor_alloc(VALUE klass) {
    pgzr_ingestor_wrapper_t *wrapper;
    VALUE obj = TypedData_Make_Struct(klass, pgzr_ingestor_wrapper_t, &pgzr_ingestor_type, wrapper);

    wrapper->ptr = NULL;
    wrapper->on_flush = Qnil;
    wrapper->callback_error = Qnil;
    wrapper->running = false;
    wrapper->strings.items = NULL;
    wrapper->strings.length = 0;
    wrapper->strings.capacity = 0;
    memset(&wrapper->config, 0, sizeof(wrapper->config));

    return obj;
}

static VALUE pgzr_processor_alloc(VALUE klass) {
    pgzr_processor_wrapper_t *wrapper;
    VALUE obj = TypedData_Make_Struct(klass, pgzr_processor_wrapper_t, &pgzr_processor_type, wrapper);

    wrapper->ptr = NULL;
    wrapper->running = false;
    wrapper->strings.items = NULL;
    wrapper->strings.length = 0;
    wrapper->strings.capacity = 0;
    memset(&wrapper->config, 0, sizeof(wrapper->config));

    return obj;
}

static VALUE pgzr_ingestor_callback_call(VALUE arg_value) {
    pgzr_flush_call_t *call = (pgzr_flush_call_t *)arg_value;
    VALUE argv[4];

    argv[0] = ULL2NUM(call->start_lsn);
    argv[1] = ULL2NUM(call->end_lsn);
    argv[2] = SIZET2NUM(call->msg_count);
    argv[3] = call->is_complete ? Qtrue : Qfalse;

    return rb_funcallv(call->wrapper->on_flush, id_call, 4, argv);
}

static void *pgzr_ingestor_callback_with_gvl(void *arg) {
    pgzr_flush_call_t *call = (pgzr_flush_call_t *)arg;
    int state = 0;

    rb_protect(pgzr_ingestor_callback_call, (VALUE)arg, &state);
    if (state != 0 && NIL_P(call->wrapper->callback_error)) {
        call->wrapper->callback_error = rb_errinfo();
        rb_set_errinfo(Qnil);
        if (call->wrapper->ptr != NULL) {
            pgzr_lib.ingestor_stop(call->wrapper->ptr);
        }
    }

    return NULL;
}

static void pgzr_ingestor_on_flush_bridge(void *context, uint64_t start_lsn, uint64_t end_lsn, size_t msg_count, bool is_complete) {
    pgzr_flush_call_t call;

    call.wrapper = (pgzr_ingestor_wrapper_t *)context;
    call.start_lsn = start_lsn;
    call.end_lsn = end_lsn;
    call.msg_count = msg_count;
    call.is_complete = is_complete;

    if (NIL_P(call.wrapper->on_flush)) {
        return;
    }

    rb_thread_call_with_gvl(pgzr_ingestor_callback_with_gvl, &call);
}

static void *pgzr_ingestor_run_without_gvl(void *arg) {
    pgzr_ingestor_run_call_t *call = (pgzr_ingestor_run_call_t *)arg;

    call->rc = pgzr_lib.ingestor_run(call->wrapper->ptr);
    return NULL;
}

static void pgzr_ingestor_ubf(void *arg) {
    pgzr_ingestor_wrapper_t *wrapper = (pgzr_ingestor_wrapper_t *)arg;

    if (wrapper->ptr != NULL) {
        pgzr_lib.ingestor_stop(wrapper->ptr);
    }
}

static void *pgzr_processor_run_without_gvl(void *arg) {
    pgzr_processor_run_call_t *call = (pgzr_processor_run_call_t *)arg;

    call->rc = pgzr_lib.processor_run(call->wrapper->ptr);
    return NULL;
}

static void pgzr_processor_ubf(void *arg) {
    pgzr_processor_wrapper_t *wrapper = (pgzr_processor_wrapper_t *)arg;

    if (wrapper->ptr != NULL) {
        pgzr_lib.processor_stop(wrapper->ptr);
    }
}

static void *pgzr_processor_process_one_without_gvl(void *arg) {
    pgzr_processor_process_one_call_t *call = (pgzr_processor_process_one_call_t *)arg;

    call->rc = pgzr_lib.processor_process_one(call->wrapper->ptr);
    return NULL;
}

static void pgzr_ingestor_require_ptr(pgzr_ingestor_wrapper_t *wrapper) {
    if (wrapper->ptr == NULL) {
        rb_raise(rb_eRuntimeError, "native ingestor handle has been freed");
    }
}

static void pgzr_processor_require_ptr(pgzr_processor_wrapper_t *wrapper) {
    if (wrapper->ptr == NULL) {
        rb_raise(rb_eRuntimeError, "native processor handle has been freed");
    }
}

static VALUE pgzr_ingestor_initialize(int argc, VALUE *argv, VALUE self) {
    VALUE kwargs;
    VALUE source;
    VALUE dest;
    VALUE source_id;
    VALUE max_batch_size;
    VALUE on_flush;
    pgzr_ingestor_wrapper_t *wrapper;

    rb_scan_args(argc, argv, "1", &kwargs);
    Check_Type(kwargs, T_HASH);

    source = pgzr_required_keyword(kwargs, id_source, "source");
    dest = pgzr_required_keyword(kwargs, id_dest, "dest");
    source_id = pgzr_required_keyword(kwargs, id_source_id, "source_id");
    max_batch_size = pgzr_hash_lookup_symbol(kwargs, id_max_batch_size);
    on_flush = pgzr_hash_lookup_symbol(kwargs, id_on_flush);

    TypedData_Get_Struct(self, pgzr_ingestor_wrapper_t, &pgzr_ingestor_type, wrapper);

    pgzr_set_conn_fields(&wrapper->strings, source, "source port", &wrapper->config.source_host, &wrapper->config.source_port, &wrapper->config.source_user, &wrapper->config.source_password, &wrapper->config.source_database, &wrapper->config.source_socket_path, &wrapper->config.source_tls_mode);
    wrapper->config.slot_name = pgzr_store_string(&wrapper->strings, pgzr_hash_lookup_symbol(source, id_slot_name));
    wrapper->config.publication_names = pgzr_store_string(&wrapper->strings, pgzr_hash_lookup_symbol(source, id_publication_names));
    wrapper->config.proto_version = pgzr_store_string(&wrapper->strings, pgzr_hash_lookup_symbol(source, id_proto_version));

    pgzr_set_conn_fields(&wrapper->strings, dest, "dest port", &wrapper->config.dest_host, &wrapper->config.dest_port, &wrapper->config.dest_user, &wrapper->config.dest_password, &wrapper->config.dest_database, &wrapper->config.dest_socket_path, &wrapper->config.dest_tls_mode);

    wrapper->config.source_id = pgzr_store_string(&wrapper->strings, source_id);
    wrapper->config.max_batch_size = pgzr_uint32_value(max_batch_size, "max_batch_size");
    wrapper->on_flush = (NIL_P(on_flush) || on_flush == Qfalse) ? Qnil : on_flush;
    wrapper->callback_error = Qnil;

    if (wrapper->on_flush != Qnil) {
        wrapper->config.on_flush = pgzr_ingestor_on_flush_bridge;
        wrapper->config.on_flush_context = wrapper;
    } else {
        wrapper->config.on_flush = NULL;
        wrapper->config.on_flush_context = NULL;
    }

    pgzr_load_library();
    wrapper->ptr = pgzr_lib.ingestor_new(&wrapper->config);
    if (wrapper->ptr == NULL) {
        pgzr_raise_last_error("pgzr_ingestor_new failed");
    }

    return self;
}

static VALUE pgzr_processor_initialize(int argc, VALUE *argv, VALUE self) {
    VALUE kwargs;
    VALUE dest;
    VALUE source_id;
    VALUE poll_interval_ms;
    VALUE metadata_message_prefix;
    VALUE metadata_table;
    pgzr_processor_wrapper_t *wrapper;

    rb_scan_args(argc, argv, "1", &kwargs);
    Check_Type(kwargs, T_HASH);

    dest = pgzr_required_keyword(kwargs, id_dest, "dest");
    source_id = pgzr_required_keyword(kwargs, id_source_id, "source_id");
    poll_interval_ms = pgzr_hash_lookup_symbol(kwargs, id_poll_interval_ms);
    metadata_message_prefix = pgzr_hash_lookup_symbol(kwargs, id_metadata_message_prefix);
    metadata_table = pgzr_hash_lookup_symbol(kwargs, id_metadata_table);

    TypedData_Get_Struct(self, pgzr_processor_wrapper_t, &pgzr_processor_type, wrapper);

    pgzr_set_conn_fields(&wrapper->strings, dest, "dest port", &wrapper->config.dest_host, &wrapper->config.dest_port, &wrapper->config.dest_user, &wrapper->config.dest_password, &wrapper->config.dest_database, &wrapper->config.dest_socket_path, &wrapper->config.dest_tls_mode);
    wrapper->config.source_id = pgzr_store_string(&wrapper->strings, source_id);
    wrapper->config.poll_interval_ms = pgzr_uint32_value(poll_interval_ms, "poll_interval_ms");
    wrapper->config.metadata_message_prefix = pgzr_store_string(&wrapper->strings, metadata_message_prefix);
    wrapper->config.metadata_table = pgzr_store_string(&wrapper->strings, metadata_table);

    pgzr_load_library();
    wrapper->ptr = pgzr_lib.processor_new(&wrapper->config);
    if (wrapper->ptr == NULL) {
        pgzr_raise_last_error("pgzr_processor_new failed");
    }

    return self;
}

static VALUE pgzr_ingestor_run(VALUE self) {
    pgzr_ingestor_wrapper_t *wrapper;
    pgzr_ingestor_run_call_t call;

    TypedData_Get_Struct(self, pgzr_ingestor_wrapper_t, &pgzr_ingestor_type, wrapper);
    pgzr_ingestor_require_ptr(wrapper);

    if (wrapper->running) {
        rb_raise(rb_eRuntimeError, "pgzr ingestor is already running");
    }

    wrapper->running = true;
    wrapper->callback_error = Qnil;
    call.wrapper = wrapper;
    call.rc = 0;

    rb_thread_call_without_gvl(pgzr_ingestor_run_without_gvl, &call, pgzr_ingestor_ubf, wrapper);

    wrapper->running = false;

    if (!NIL_P(wrapper->callback_error)) {
        VALUE error = wrapper->callback_error;
        wrapper->callback_error = Qnil;
        rb_exc_raise(error);
    }

    if (call.rc != 0) {
        pgzr_raise_last_error("pgzr_ingestor_run failed");
    }

    return Qnil;
}

static VALUE pgzr_ingestor_stop(VALUE self) {
    pgzr_ingestor_wrapper_t *wrapper;

    TypedData_Get_Struct(self, pgzr_ingestor_wrapper_t, &pgzr_ingestor_type, wrapper);
    if (wrapper->ptr != NULL) {
        pgzr_lib.ingestor_stop(wrapper->ptr);
    }

    return Qnil;
}

static VALUE pgzr_ingestor_free(VALUE self) {
    pgzr_ingestor_wrapper_t *wrapper;

    TypedData_Get_Struct(self, pgzr_ingestor_wrapper_t, &pgzr_ingestor_type, wrapper);

    if (wrapper->running) {
        rb_raise(rb_eRuntimeError, "cannot free an ingestor while run is active");
    }

    if (wrapper->ptr != NULL) {
        pgzr_lib.ingestor_free(wrapper->ptr);
        wrapper->ptr = NULL;
    }

    pgzr_string_store_free(&wrapper->strings);
    memset(&wrapper->config, 0, sizeof(wrapper->config));
    wrapper->on_flush = Qnil;
    wrapper->callback_error = Qnil;

    return Qnil;
}

static VALUE pgzr_processor_run(VALUE self) {
    pgzr_processor_wrapper_t *wrapper;
    pgzr_processor_run_call_t call;

    TypedData_Get_Struct(self, pgzr_processor_wrapper_t, &pgzr_processor_type, wrapper);
    pgzr_processor_require_ptr(wrapper);

    if (wrapper->running) {
        rb_raise(rb_eRuntimeError, "pgzr processor is already running");
    }

    wrapper->running = true;
    call.wrapper = wrapper;
    call.rc = 0;

    rb_thread_call_without_gvl(pgzr_processor_run_without_gvl, &call, pgzr_processor_ubf, wrapper);

    wrapper->running = false;

    if (call.rc != 0) {
        pgzr_raise_last_error("pgzr_processor_run failed");
    }

    return Qnil;
}

static VALUE pgzr_processor_process_one(VALUE self) {
    pgzr_processor_wrapper_t *wrapper;
    pgzr_processor_process_one_call_t call;

    TypedData_Get_Struct(self, pgzr_processor_wrapper_t, &pgzr_processor_type, wrapper);
    pgzr_processor_require_ptr(wrapper);

    call.wrapper = wrapper;
    call.rc = 0;

    rb_thread_call_without_gvl(pgzr_processor_process_one_without_gvl, &call, RUBY_UBF_IO, NULL);

    if (call.rc == -1) {
        pgzr_raise_last_error("pgzr_processor_process_one failed");
    }

    return call.rc == 1 ? Qtrue : Qfalse;
}

static VALUE pgzr_processor_stop(VALUE self) {
    pgzr_processor_wrapper_t *wrapper;

    TypedData_Get_Struct(self, pgzr_processor_wrapper_t, &pgzr_processor_type, wrapper);
    if (wrapper->ptr != NULL) {
        pgzr_lib.processor_stop(wrapper->ptr);
    }

    return Qnil;
}

static VALUE pgzr_processor_free(VALUE self) {
    pgzr_processor_wrapper_t *wrapper;

    TypedData_Get_Struct(self, pgzr_processor_wrapper_t, &pgzr_processor_type, wrapper);

    if (wrapper->running) {
        rb_raise(rb_eRuntimeError, "cannot free a processor while run is active");
    }

    if (wrapper->ptr != NULL) {
        pgzr_lib.processor_free(wrapper->ptr);
        wrapper->ptr = NULL;
    }

    pgzr_string_store_free(&wrapper->strings);
    memset(&wrapper->config, 0, sizeof(wrapper->config));

    return Qnil;
}

void Init_pgzr_ext(void) {
    mPGZR = rb_define_module("PGZR");

    cIngestor = rb_define_class_under(mPGZR, "Ingestor", rb_cObject);
    rb_define_alloc_func(cIngestor, pgzr_ingestor_alloc);
    rb_define_method(cIngestor, "initialize", pgzr_ingestor_initialize, -1);
    rb_define_method(cIngestor, "run", pgzr_ingestor_run, 0);
    rb_define_method(cIngestor, "stop", pgzr_ingestor_stop, 0);
    rb_define_method(cIngestor, "free", pgzr_ingestor_free, 0);

    cProcessor = rb_define_class_under(mPGZR, "Processor", rb_cObject);
    rb_define_alloc_func(cProcessor, pgzr_processor_alloc);
    rb_define_method(cProcessor, "initialize", pgzr_processor_initialize, -1);
    rb_define_method(cProcessor, "run", pgzr_processor_run, 0);
    rb_define_method(cProcessor, "process_one", pgzr_processor_process_one, 0);
    rb_define_method(cProcessor, "stop", pgzr_processor_stop, 0);
    rb_define_method(cProcessor, "free", pgzr_processor_free, 0);

    rb_define_singleton_method(mPGZR, "last_error", pgzr_last_error_method, 0);
    rb_define_singleton_method(mPGZR, "tls_mode_value", pgzr_tls_mode_value_method, 1);

    id_call = rb_intern("call");
    id_host = rb_intern("host");
    id_port = rb_intern("port");
    id_user = rb_intern("user");
    id_password = rb_intern("password");
    id_database = rb_intern("database");
    id_socket_path = rb_intern("socket_path");
    id_tls_mode = rb_intern("tls_mode");
    id_source = rb_intern("source");
    id_dest = rb_intern("dest");
    id_source_id = rb_intern("source_id");
    id_max_batch_size = rb_intern("max_batch_size");
    id_on_flush = rb_intern("on_flush");
    id_poll_interval_ms = rb_intern("poll_interval_ms");
    id_metadata_message_prefix = rb_intern("metadata_message_prefix");
    id_metadata_table = rb_intern("metadata_table");
    id_slot_name = rb_intern("slot_name");
    id_publication_names = rb_intern("publication_names");
    id_proto_version = rb_intern("proto_version");
}
