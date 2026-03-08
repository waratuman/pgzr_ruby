# frozen_string_literal: true

require "test_helper"
require "pgzr"

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

class PGZRNativeTest < Minitest::Test
  def test_native_classes_are_available_on_pgzr
    assert defined?(PGZR::Ingestor)
    assert defined?(PGZR::Processor)
  end

  def test_native_instance_methods_are_available
    assert_respond_to PGZR::Ingestor.allocate, :run
    assert_respond_to PGZR::Ingestor.allocate, :stop
    assert_respond_to PGZR::Processor.allocate, :run
    assert_respond_to PGZR::Processor.allocate, :process_one
    assert_respond_to PGZR::Processor.allocate, :stop
  end
end

class PGZRTlsModeTest < Minitest::Test
  def test_disable
    assert_equal 0, PGZR.tls_mode_value(:disable)
  end

  def test_prefer
    assert_equal 1, PGZR.tls_mode_value(:prefer)
  end

  def test_require
    assert_equal 2, PGZR.tls_mode_value(:require)
  end

  def test_nil_defaults_to_disable
    assert_equal 0, PGZR.tls_mode_value(nil)
  end

  def test_string_mode
    assert_equal 1, PGZR.tls_mode_value("prefer")
  end

  def test_verify_full
    assert_equal 3, PGZR.tls_mode_value(:verify_full)
  end

  def test_unknown_mode_raises
    assert_raises(ArgumentError) { PGZR.tls_mode_value(:bogus) }
  end
end

class PGZRNativeSmokeTest < Minitest::Test
  def test_ingestor_constructor_raises_ruby_error_instead_of_crashing
    skip "native smoke test disabled" unless ENV["PGZR_RUN_NATIVE_SMOKE"] == "1"
    skip "PGZR_LIB_PATH not set" if ENV["PGZR_LIB_PATH"].to_s.empty?

    error = assert_raises(PGZR::Error) do
      PGZR::Ingestor.new(
        source: {
          host: "127.0.0.1",
          port: 1,
          user: "postgres",
          database: "postgres",
          slot_name: "test_slot",
          publication_names: "test_pub",
          proto_version: "1"
        },
        dest: {
          host: "127.0.0.1",
          port: 1,
          user: "postgres",
          database: "postgres"
        },
        source_id: "00000000-0000-0000-0000-000000000001"
      )
    end

    refute_empty error.message
  end
end
