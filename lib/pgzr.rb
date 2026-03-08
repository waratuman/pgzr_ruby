# frozen_string_literal: true

require_relative "pgzr/version"
require_relative "pgzr/error"
begin
  require "pgzr/pgzr_ext"
rescue LoadError
  require_relative "../ext/pgzr/pgzr_ext"
end

module PGZR
  # :singleton-method: last_error
  #
  # call-seq:
  #   PGZR.last_error -> String or nil
  #
  # Returns the most recent error message reported by +libpgzr+ on the current
  # thread, or +nil+ when no native error has been recorded.
  #
  # This is mainly useful when debugging low-level native failures.
  #
  # :singleton-method: tls_mode_value
  #
  # call-seq:
  #   PGZR.tls_mode_value(mode) -> Integer
  #
  # Converts a TLS mode symbol or string into the integer constant expected by
  # +libpgzr+.
  #
  # Accepted values are +:disable+, +:prefer+, +:require+, and
  # +:verify_full+. Raises +ArgumentError+ for unknown values.

  # Streams WAL changes from a source PostgreSQL database and writes packed
  # batches into the destination database.
  class Ingestor
    # :method: initialize
    #
    # call-seq:
    #   PGZR::Ingestor.new(source:, dest:, source_id:, max_batch_size: 0, on_flush: nil) -> ingestor
    #
    # Creates a native ingestor.
    #
    # The +source+ and +dest+ hashes accept these keys:
    # [+:host+] PostgreSQL host name.
    # [+:port+] PostgreSQL port number.
    # [+:user+] PostgreSQL user name.
    # [+:password+] PostgreSQL password.
    # [+:database+] Database name.
    # [+:socket_path+] Unix socket directory.
    # [+:tls_mode+] One of +:disable+, +:prefer+, +:require+, or +:verify_full+.
    #
    # The +source+ hash also accepts:
    # [+:slot_name+] Replication slot name.
    # [+:publication_names+] Comma-separated publication names.
    # [+:proto_version+] Replication protocol version string.
    #
    # +source_id+ identifies the stream in the destination schema.
    #
    # When +on_flush+ is provided, it is called with
    # <tt>(start_lsn, end_lsn, msg_count, complete)</tt> after each successful
    # flush.
    #
    # :method: run
    #
    # call-seq:
    #   ingestor.run -> nil
    #
    # Starts the blocking ingest loop.
    #
    # Raises +PGZR::Error+ if the native ingestor fails.
    #
    # :method: stop
    #
    # call-seq:
    #   ingestor.stop -> nil
    #
    # Requests a graceful stop for a running ingestor.
    #
    # :method: free
    #
    # call-seq:
    #   ingestor.free -> nil
    #
    # Releases the native ingestor immediately.
    #
    # Most callers can rely on garbage collection instead of calling this
    # method directly.
  end

  # Reads packed WAL batches from the destination database and decodes them
  # into structured transactions.
  class Processor
    # :method: initialize
    #
    # call-seq:
    #   PGZR::Processor.new(dest:, source_id:, poll_interval_ms: 0, metadata_message_prefix: nil, metadata_table: nil) -> processor
    #
    # Creates a native processor.
    #
    # The +dest+ hash accepts the same connection keys as +PGZR::Ingestor+.
    #
    # +source_id+ identifies which stream to process.
    #
    # +poll_interval_ms+ controls how often the processor polls when no pending
    # batches are available.
    #
    # +metadata_message_prefix+ and +metadata_table+ enable optional metadata
    # extraction features in +libpgzr+.
    #
    # :method: run
    #
    # call-seq:
    #   processor.run -> nil
    #
    # Starts the blocking processor loop.
    #
    # Raises +PGZR::Error+ if the native processor fails.
    #
    # :method: process_one
    #
    # call-seq:
    #   processor.process_one -> true or false
    #
    # Processes one pending batch.
    #
    # Returns +true+ when a batch was processed and +false+ when no batch was
    # available.
    #
    # :method: stop
    #
    # call-seq:
    #   processor.stop -> nil
    #
    # Requests a graceful stop for a running processor.
    #
    # :method: free
    #
    # call-seq:
    #   processor.free -> nil
    #
    # Releases the native processor immediately.
    #
    # Most callers can rely on garbage collection instead of calling this
    # method directly.
  end
end
