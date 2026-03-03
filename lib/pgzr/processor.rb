# frozen_string_literal: true

module PGZR
  class Processor
    def initialize(dest:, source_id:, poll_interval_ms: 0)
      @strings = []
      config = FFI::ProcessorConfig.new

      set_conn_fields(config, :dest, dest)
      config[:source_id]        = pin_string(source_id)
      config[:poll_interval_ms] = poll_interval_ms

      @ptr = FFI.pgzr_processor_new(config)
      raise Error, FFI.last_error || "pgzr_processor_new failed" if @ptr.null?

      ObjectSpace.define_finalizer(self, self.class.release(@ptr))
    end

    def run
      rc = FFI.pgzr_processor_run(@ptr)
      raise Error, FFI.last_error || "pgzr_processor_run failed" unless rc == 0
    end

    def process_one
      rc = FFI.pgzr_processor_process_one(@ptr)
      raise Error, FFI.last_error || "pgzr_processor_process_one failed" if rc == -1
      rc == 1
    end

    def stop
      FFI.pgzr_processor_stop(@ptr)
    end

    def self.release(ptr)
      proc { FFI.pgzr_processor_free(ptr) }
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
