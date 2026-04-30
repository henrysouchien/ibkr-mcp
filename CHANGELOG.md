# Changelog

All notable changes to `interactive-brokers-mcp` are documented here. Entries follow the
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) convention.

## [0.2.4] — 2026-04-30

### Fixed
- **`app_platform.api_budget.guard_call` import wrapped in `try/except ImportError`** at `ibkr/_budget.py:8`, with a no-op passthrough fallback. Standalone installs run without monorepo budget enforcement; monorepo runtime is unchanged.
- **`BudgetExceededError` exception class and `COST_PER_CALL` lookup table now vendored into `ibkr/_shared/`** at sync time. The four `except BudgetExceededError:` clauses in `ibkr/{connection,market_data,flex,account}.py` and the `COST_PER_CALL` lookup in `ibkr/_budget.py:9` now resolve in standalone installs without `app_platform` or top-level `config/` on `sys.path`.

## [0.2.3] — 2026-04-30

### Fixed
- **Cross-package `utils/timeseries_store.py` import vendored into `ibkr/_shared/`** at sync time, with `utils.X` → `ibkr._shared.X` rewrites. Prior versions assumed the monorepo `utils/` package would be on `sys.path`, which it isn't for users installing from PyPI.
- **`ibkr/timeseries_cache.py` was missing entirely from prior dist publishes** — the sync script hadn't run since the file was added in source. Now properly shipped.

## [0.2.2]

### Added
- FOP (Futures-on-Options) snapshot support.
- Strike and right fields in contract details.
- `ibkr_mcp` alias for the entry point.

### Changed
- Added version upper bounds to all dependencies in `pyproject.toml`.

## [0.2.1]

### Changed
- PyPI license metadata republish (no functional changes).

## [0.2.0]

### Changed
- License switched from MIT to PolyForm Noncommercial 1.0.0.

## [0.1.0]

### Added
- Initial release — MCP server for Interactive Brokers Gateway with 6 tools for market data and account access via Claude.
