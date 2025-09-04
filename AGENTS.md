# Repository Guidelines

## Project Structure & Module Organization
- `src/main.rs`: HTTP query service entrypoint.
- `src/lib.rs`: exports `Database` and ties core modules.
- `src/memtable.rs`: in-memory write buffer before flush.
- `src/wal.rs`: write-ahead log for durability.
- `src/sstable.rs`: on-disk sorted string tables.
- `src/query.rs`: minimal SQL parsing/execution.
- `src/storage/`: async backends (local FS, S3).
- `tests/`: unit + integration tests demonstrating usage.
- `Dockerfile`, `docker-compose.*`: containerized dev/test helpers.

## Build, Test, and Development Commands
- Build: `cargo build` — compile the project.
- Run locally: `cargo run` — start the HTTP service.
- Test: `cargo test` — run unit and integration tests.
- Format: `cargo fmt --all` — format Rust code.
- Lint (optional but encouraged): `cargo clippy --all-targets -D warnings`.
- Docker (example): `docker build -t cass .` then `docker run -p 8080:8080 cass`.

## Coding Style & Naming Conventions
- Use `cargo fmt` before committing; prefer idiomatic Rust.
- Keep functions small and focused; avoid unnecessary generics.
- Names: modules/files `snake_case`; types/traits `PascalCase`; functions/vars `snake_case`; consts `SCREAMING_SNAKE_CASE`.
- Error handling: prefer `Result<T, E>` with context; avoid `unwrap()` in non-test code.

## Testing Guidelines
- Framework: Rust’s built-in test harness.
- Locations: unit tests co-located with modules using `#[cfg(test)]`; integration tests in `tests/`.
- Conventions: name tests after behavior, e.g., `flush_writes_to_sstable`.
- Run subsets: `cargo test <name>`; add tests for bug fixes and new features.

## Commit & Pull Request Guidelines
- Commits: imperative mood, concise subject (≤72 chars), optional scope, e.g., `wal: prevent partial record read`.
- PRs: clear description, link issues (`Closes #123`), rationale, testing notes, and any SQL/API changes.
- Requirements: all tests pass, code formatted, docs updated (this file or module docs) when behavior changes.

## Security & Configuration Tips
- Do not commit secrets; use environment variables for backend credentials (e.g., S3).
- Prefer local filesystem backend for development; validate S3 settings in a disposable environment.
- Log minimally in production; avoid leaking user data in errors.
