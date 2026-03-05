# frozen_string_literal: true

require "ffi"

module PGZR
  module FFI
    extend ::FFI::Library

    lib_path = ENV["PGZR_LIB_PATH"]
    ffi_lib(lib_path || "pgzr")

    # -- Structs (extern struct layout from cabi.zig) --------------------------

    class IngestConfig < ::FFI::Struct
      layout(
        :source_host,        :pointer,
        :source_port,        :uint16,
        :source_user,        :pointer,
        :source_password,    :pointer,
        :source_database,    :pointer,
        :source_socket_path, :pointer,
        :source_tls_mode,    :uint8,
        :slot_name,          :pointer,
        :publication_names,  :pointer,
        :proto_version,      :pointer,
        :dest_host,          :pointer,
        :dest_port,          :uint16,
        :dest_user,          :pointer,
        :dest_password,      :pointer,
        :dest_database,      :pointer,
        :dest_socket_path,   :pointer,
        :dest_tls_mode,      :uint8,
        :source_id,          :pointer,
        :max_batch_size,     :uint32
      )
    end

    class ProcessorConfig < ::FFI::Struct
      layout(
        :dest_host,                  :pointer,
        :dest_port,                  :uint16,
        :dest_user,                  :pointer,
        :dest_password,              :pointer,
        :dest_database,              :pointer,
        :dest_socket_path,           :pointer,
        :dest_tls_mode,              :uint8,
        :source_id,                  :pointer,
        :poll_interval_ms,           :uint32,
        :metadata_message_prefix,    :pointer,
        :metadata_table,             :pointer
      )
    end

    # -- Functions -------------------------------------------------------------

    # pgzr_last_error takes a pointer to size_t (out-param for length),
    # returns a pointer to the error string bytes (not null-terminated).
    attach_function :pgzr_last_error,            [:pointer],  :pointer
    attach_function :pgzr_ingestor_new,          [:pointer],  :pointer
    attach_function :pgzr_ingestor_run,          [:pointer],  :int,  blocking: true
    attach_function :pgzr_ingestor_stop,         [:pointer],  :void
    attach_function :pgzr_ingestor_free,         [:pointer],  :void
    attach_function :pgzr_processor_new,         [:pointer],  :pointer
    attach_function :pgzr_processor_run,         [:pointer],  :int,  blocking: true
    attach_function :pgzr_processor_process_one, [:pointer],  :int
    attach_function :pgzr_processor_stop,        [:pointer],  :void
    attach_function :pgzr_processor_free,        [:pointer],  :void

    # -- Helpers ---------------------------------------------------------------

    # Reads the thread-local error message from pgzr.
    def self.last_error
      len_ptr = ::FFI::MemoryPointer.new(:size_t)
      str_ptr = pgzr_last_error(len_ptr)
      len = len_ptr.read(:size_t)
      return nil if str_ptr.null? || len == 0
      str_ptr.read_string(len)
    end

    TLS_MODES = { disable: 0, prefer: 1, require: 2 }.freeze

    # Converts a Ruby symbol/string TLS mode to the uint8 value.
    def self.tls_mode_value(mode)
      return 0 if mode.nil?
      val = TLS_MODES[mode.to_sym]
      raise ArgumentError, "unknown TLS mode: #{mode.inspect} (expected :disable, :prefer, or :require)" unless val
      val
    end
  end
end
