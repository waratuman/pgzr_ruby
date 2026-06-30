# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

## [0.4.1]

Track libpgzr 0.4.1. The exported C ABI is unchanged from 0.4.0
(`pgzr_abi_version` still reports `0x00040000` and both config struct
layouts are identical), so no native binding changes were required. The
underlying library picks up several fixes:

- Concurrent processors no longer double-process a batch during zombie
  recovery.
- The CSPRNG now calls `getrandom(2)` directly, removing a class of
  `dlopen`-plus-threads crashes during TLS handshakes from FFI hosts.
- A zero-byte response to `SSLRequest` is treated as a closed connection.
- `pgzr_abi_version` is exported, which the load-time ABI check below
  relies on. This symbol is absent from the 0.4.0 release, so the gem now
  effectively requires libpgzr 0.4.1 or newer.

### Added

- **Libpgzr ABI version check at load time.** The extension now resolves
  `pgzr_abi_version` immediately after `dlopen` and raises a clear
  `LoadError` when the symbol is missing (pre-0.4.1 library) or returns
  an unexpected value. Previously a version skew between the gem's
  expected struct layout and the deployed `libpgzr` would manifest as a
  segfault inside `pgzr_processor_new` / `pgzr_ingestor_new` when Zig
  dereferenced a config field whose offset had shifted.

## [0.4.0]

- Track libpgzr 0.4.0: removed `source_id` from `PGZR::Processor` (now
  source-agnostic). Use `PGZR::Ingestor`'s `source_id` to tag streams.

## Earlier

See `git log` for releases prior to the introduction of this file.
