# frozen_string_literal: true

require_relative "lib/pgzr/version"

Gem::Specification.new do |spec|
  spec.name = "pgzr"
  spec.version = PGZR::VERSION
  spec.authors = ["waratuman"]
  spec.summary = "Ruby bindings for pgzr PostgreSQL logical replication"
  spec.description = "FFI wrapper around libpgzr for PostgreSQL logical replication ingestion and processing."
  spec.homepage = "https://github.com/waratuman/pgzr_ruby"
  spec.required_ruby_version = ">= 3.0.0"

  spec.files = Dir["lib/**/*.rb"] + ["README.md"]
  spec.require_paths = ["lib"]

  spec.add_dependency "ffi", "~> 1.0"

  spec.add_development_dependency "minitest", "~> 5.0"
  spec.add_development_dependency "rake", "~> 13.0"
end
