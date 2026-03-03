# pgzr

Ruby bindings for [pgzr](https://github.com/waratuman/pgzr), a PostgreSQL
logical replication library. Uses FFI to wrap the `libpgzr` C ABI.

## Prerequisites

- Ruby >= 3.0
- `libpgzr` shared library (`libpgzr.dylib` on macOS, `libpgzr.so` on Linux)

Set `PGZR_LIB_PATH` to the full path of the library if it is not installed in a
standard system library location:

```sh
export PGZR_LIB_PATH=/path/to/libpgzr.dylib
```

## Installation

Add to your Gemfile:

```ruby
gem "pgzr"
```

Then run `bundle install`.

## Usage

### Ingestor

Streams WAL changes from a source PostgreSQL database and stores packed batches
in a destination database.

```ruby
require "pgzr"

ingestor = PGZR::Ingestor.new(
  source: {
    host: "127.0.0.1",
    port: 5432,
    user: "postgres",
    database: "source_db",
    slot_name: "my_slot",
    publication_names: "my_pub"
  },
  dest: {
    host: "127.0.0.1",
    port: 5432,
    user: "postgres",
    database: "dest_db"
  },
  source_id: "00000000-0000-0000-0000-000000000001"
)

# Run in a thread so you can stop it from a signal handler
thread = Thread.new { ingestor.run }

Signal.trap("INT") { ingestor.stop }

thread.join
```

### Processor

Reads batches from the destination database and decodes them into structured
transactions.

```ruby
require "pgzr"

processor = PGZR::Processor.new(
  dest: {
    host: "127.0.0.1",
    port: 5432,
    user: "postgres",
    database: "dest_db"
  },
  source_id: "00000000-0000-0000-0000-000000000001"
)

# Blocking loop
processor.run

# Or process one batch at a time
processed = processor.process_one  # => true if a batch was processed, false if none available
```

### TLS Modes

Both `source` and `dest` connection hashes accept a `:tls_mode` key:

- `:disable` (default) -- no TLS
- `:prefer` -- use TLS if available
- `:require` -- require TLS

```ruby
PGZR::Ingestor.new(
  source: { host: "db.example.com", tls_mode: :require, ... },
  dest:   { host: "127.0.0.1", ... },
  source_id: "..."
)
```

## Development

```sh
bundle install
rake test
```

## License

All rights reserved.
