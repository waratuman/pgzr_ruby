# frozen_string_literal: true

module PGZR
  class Ingestor
    def initialize(source:, dest:, source_id:, max_batch_size: 0, on_flush: nil)
      @strings = []
      config = FFI::IngestConfig.new

      set_conn_fields(config, :source, source)
      config[:slot_name]          = pin_string(source[:slot_name])
      config[:publication_names]  = pin_string(source[:publication_names])
      config[:proto_version]      = pin_string(source[:proto_version])

      set_conn_fields(config, :dest, dest)

      config[:source_id]         = pin_string(source_id)
      config[:max_batch_size]    = max_batch_size
      config[:on_flush_context]  = ::FFI::Pointer::NULL

      if on_flush
        fn = ::FFI::Function.new(:void, [:pointer, :uint64, :uint64, :size_t, :bool]) do |_ctx, start_lsn, end_lsn, msg_count, is_complete|
          on_flush.call(start_lsn, end_lsn, msg_count, is_complete)
        end
        @strings << fn
        config[:on_flush] = fn
      end

      @ptr = FFI.pgzr_ingestor_new(config)
      raise Error, FFI.last_error || "pgzr_ingestor_new failed" if @ptr.null?

      ObjectSpace.define_finalizer(self, self.class.release(@ptr))
    end

    def run
      rc = FFI.pgzr_ingestor_run(@ptr)
      raise Error, FFI.last_error || "pgzr_ingestor_run failed" unless rc == 0
    end

    def stop
      FFI.pgzr_ingestor_stop(@ptr)
    end

    def self.release(ptr)
      proc { FFI.pgzr_ingestor_free(ptr) }
    end

    private

    def set_conn_fields(config, prefix, opts)
      config[:"#{prefix}_host"]        = pin_string(opts[:host])
      config[:"#{prefix}_port"]        = opts[:port] || 0
      config[:"#{prefix}_user"]        = pin_string(opts[:user])
      config[:"#{prefix}_password"]    = pin_string(opts[:password])
      config[:"#{prefix}_database"]    = pin_string(opts[:database])
      config[:"#{prefix}_socket_path"] = pin_string(opts[:socket_path])
      config[:"#{prefix}_tls_mode"]    = FFI.tls_mode_value(opts[:tls_mode])
    end

    def pin_string(str)
      return ::FFI::Pointer::NULL if str.nil?
      mp = ::FFI::MemoryPointer.from_string(str.to_s)
      @strings << mp
      mp
    end
  end
end
