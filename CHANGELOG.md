# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added

- **Libpgzr ABI version check at load time.** The extension now resolves
  `pgzr_abi_version` immediately after `dlopen` and raises a clear
  `LoadError` when the symbol is missing (pre-0.4.0 library) or returns
  an unexpected value. Previously a version skew between the gem's
  expected struct layout and the deployed `libpgzr` would manifest as a
  segfault inside `pgzr_processor_new` / `pgzr_ingestor_new` when Zig
  dereferenced a config field whose offset had shifted.

## [0.4.0]

- Track libpgzr 0.4.0: removed `source_id` from `PGZR::Processor` (now
  source-agnostic). Use `PGZR::Ingestor`'s `source_id` to tag streams.

## Earlier

See `git log` for releases prior to the introduction of this file.
