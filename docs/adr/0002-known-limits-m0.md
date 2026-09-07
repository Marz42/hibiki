# ADR-0002: Known limits (M0)

## Status

Accepted — 2026-09-07

## Limits

- No real model adapter, sandbox, or external provider
- ContextManifest is recorded; materialization is a stub
- Workspace fencing uses in-memory Fake writers, not OS processes
- CLI injects AuthContext; no browser session or CSRF
- Single Core instance lock is a SQLite/file lock on the data directory

These are intentional M0 boundaries, not silent weakenings of authorization rules.
