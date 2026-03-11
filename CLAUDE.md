# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

See `AGENTS.md` for detailed architecture, coding conventions, anti-patterns, and where-to-look tables. Subdirectory `AGENTS.md` files provide component-specific context.

## Quick Reference: Build & Test Commands

This project uses **Bazel** as the build system and **Just** as a task runner.

```bash
just build                    # Build everything (alias: just b)
just test                     # Run all tests (alias: just t)
just format                   # Format all code (alias: just f)
just compile-commands         # Generate compile_commands.json for clangd
just prepare                  # Install all dev dependencies
```

### Running a Single Test

The `@` suffix is **required** in test target names:

```bash
just test //src/workerd/api/tests:encoding-test@
just stream-test //src/workerd/api/tests:encoding-test@   # streams output for debugging
just node-test zlib                                        # Node.js compat test
just wpt-test urlpattern                                   # Web Platform Test
```

Each test auto-generates 3 variants: `name@` (oldest compat), `name@all-compat-flags`, `name@all-autogates`.

To find available test targets: `bazel query //src/workerd/api/tests:all`

### Scaffolding New Tests

```bash
just new-test //src/workerd/api/tests:my-test    # scaffold .wd-test + JS file
just new-wpt-test urlpattern                      # scaffold WPT test
```

### Linting & Other Tools

```bash
just lint                          # ESLint on TypeScript sources
just clippy dns                    # Rust clippy (e.g., just clippy jsg-macros)
just clang-tidy //src/rust/jsg:ffi # C++ linting
just bench mimetype                # Run a benchmark
```

## Key Facts

- **C++ files use `.c++` extension** (not `.cpp`), test files use `-test` suffix
- **KJ types over STL**: `kj::String`, `kj::Array<T>`, `kj::Own<T>`, `kj::Maybe<T>`, etc.
- **JSG macros** bind C++ to JavaScript (V8): `JSG_RESOURCE_TYPE`, `JSG_METHOD`, `JSG_STRUCT`
- **Cap'n Proto** for config files (`.capnp`) and serialization
- **pnpm** for TypeScript/JavaScript package management
- **Backward compatibility** is a hard constraint: features cannot be removed once deployed
- Compatibility flags in `src/workerd/io/compatibility-date.capnp`, autogates in `src/workerd/util/autogate.h`
- Dependencies are vendored via Bazel into `external/`; patches in `patches/`

## Repository Structure (Key Paths)

| Path | Purpose |
|------|---------|
| `src/workerd/api/` | Runtime JS APIs (HTTP, crypto, streams, etc.) |
| `src/workerd/api/node/` | Node.js compat layer (C++ side) |
| `src/workerd/io/` | I/O subsystem, worker lifecycle, actor storage |
| `src/workerd/jsg/` | JavaScript Glue: C++/V8 binding infrastructure |
| `src/workerd/server/` | Server binary, config parsing |
| `src/node/` | Node.js compat layer (TypeScript side) |
| `src/cloudflare/` | Cloudflare-specific APIs (TypeScript) |
| `build/` | Custom Bazel rules (`wd_test.bzl`, `wd_cc_library.bzl`, etc.) |
| `types/` | TypeScript type definitions |
