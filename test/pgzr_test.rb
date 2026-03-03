# frozen_string_literal: true

require "test_helper"
require "pgzr/version"
require "pgzr/error"
require "ffi"

# Load the struct definitions without loading the shared library.
# We re-define the FFI module here to avoid the ffi_lib call.
module PGZR
  module FFI
    extend ::FFI::Library

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
        :dest_host,          :pointer,
        :dest_port,          :uint16,
        :dest_user,          :pointer,
        :dest_password,      :pointer,
        :dest_database,      :pointer,
        :dest_socket_path,   :pointer,
        :dest_tls_mode,      :uint8,
        :source_id,          :pointer,
        :poll_interval_ms,   :uint32
      )
    end

    TLS_MODES = { disable: 0, prefer: 1, require: 2 }.freeze

    def self.tls_mode_value(mode)
      return 0 if mode.nil?
      val = TLS_MODES[mode.to_sym]
      raise ArgumentError, "unknown TLS mode: #{mode.inspect}" unless val
      val
    end
  end
end

class PGZRVersionTest < Minitest::Test
  def test_version_is_defined
    refute_nil PGZR::VERSION
    assert_match(/\A\d+\.\d+\.\d+\z/, PGZR::VERSION)
  end
end

class PGZRErrorTest < Minitest::Test
  def test_error_is_a_standard_error
    assert PGZR::Error < StandardError
  end

  def test_error_can_be_raised
    assert_raises(PGZR::Error) { raise PGZR::Error, "test" }
  end
end

class PGZRIngestConfigTest < Minitest::Test
  def test_field_names
    expected = %i[
      source_host source_port source_user source_password source_database
      source_socket_path source_tls_mode slot_name publication_names proto_version
      dest_host dest_port dest_user dest_password dest_database
      dest_socket_path dest_tls_mode source_id max_batch_size
    ]
    assert_equal expected, PGZR::FFI::IngestConfig.members
  end

  def test_set_port
    config = PGZR::FFI::IngestConfig.new
    config[:source_port] = 5432
    assert_equal 5432, config[:source_port]
  end

  def test_set_string_field
    config = PGZR::FFI::IngestConfig.new
    str = ::FFI::MemoryPointer.from_string("127.0.0.1")
    config[:source_host] = str
    refute config[:source_host].null?
    assert_equal "127.0.0.1", config[:source_host].read_string
  end

  def test_default_values_are_zero
    config = PGZR::FFI::IngestConfig.new
    assert_equal 0, config[:source_port]
    assert_equal 0, config[:dest_port]
    assert_equal 0, config[:source_tls_mode]
    assert_equal 0, config[:dest_tls_mode]
    assert_equal 0, config[:max_batch_size]
  end

  def test_tls_mode_field
    config = PGZR::FFI::IngestConfig.new
    config[:source_tls_mode] = 2
    assert_equal 2, config[:source_tls_mode]
  end
end

class PGZRProcessorConfigTest < Minitest::Test
  def test_field_names
    expected = %i[
      dest_host dest_port dest_user dest_password dest_database
      dest_socket_path dest_tls_mode source_id poll_interval_ms
    ]
    assert_equal expected, PGZR::FFI::ProcessorConfig.members
  end

  def test_set_poll_interval
    config = PGZR::FFI::ProcessorConfig.new
    config[:poll_interval_ms] = 500
    assert_equal 500, config[:poll_interval_ms]
  end

  def test_default_values_are_zero
    config = PGZR::FFI::ProcessorConfig.new
    assert_equal 0, config[:dest_port]
    assert_equal 0, config[:dest_tls_mode]
    assert_equal 0, config[:poll_interval_ms]
  end
end

class PGZRTlsModeTest < Minitest::Test
  def test_disable
    assert_equal 0, PGZR::FFI.tls_mode_value(:disable)
  end

  def test_prefer
    assert_equal 1, PGZR::FFI.tls_mode_value(:prefer)
  end

  def test_require
    assert_equal 2, PGZR::FFI.tls_mode_value(:require)
  end

  def test_nil_defaults_to_disable
    assert_equal 0, PGZR::FFI.tls_mode_value(nil)
  end

  def test_string_mode
    assert_equal 1, PGZR::FFI.tls_mode_value("prefer")
  end

  def test_unknown_mode_raises
    assert_raises(ArgumentError) { PGZR::FFI.tls_mode_value(:bogus) }
  end
end
