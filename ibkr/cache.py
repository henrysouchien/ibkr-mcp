"""Disk cache helpers for IBKR monthly market-data series.

Called by:
- ``ibkr/market_data.py`` fetch paths for read-through/write-through caching.
- Admin/debug paths that inspect or clear IBKR cache files.

Contract notes:
- Cache key encodes symbol/instrument/request-shape/date-window.
- Current-month windows use TTL eviction; historical windows persist until
  explicit cleanup.
- Corrupt cache files are removed on read to keep callers fail-open.
"""

from __future__ import annotations

import hashlib
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd
from pandas.errors import EmptyDataError, ParserError


CURRENT_MONTH_TTL_HOURS = 4


def _project_root() -> Path:
    """Return default cache directory with portable fallback behavior."""
    configured = os.getenv("IBKR_CACHE_DIR")
    if configured:
        return Path(configured).expanduser().resolve()

    package_dir = Path(__file__).resolve().parent
    monorepo_root = package_dir.parent
    if (monorepo_root / "settings.py").is_file():
        return monorepo_root / "cache" / "ibkr"

    return Path.home() / ".cache" / "ibkr-mcp"


def _cache_dir(base_dir: str | Path | None = None) -> Path:
    """Resolve and ensure the IBKR disk-cache directory."""
    if base_dir is not None:
        root = Path(base_dir).expanduser().resolve()
        path = root / "cache" / "ibkr"
    else:
        path = _project_root()
    path.mkdir(parents=True, exist_ok=True)
    return path


def _to_timestamp(value: Any) -> pd.Timestamp:
    """Normalize datetime-like values to naive UTC ``pd.Timestamp``."""

    ts = pd.Timestamp(value)
    if ts.tz is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts


def _includes_current_month(
    start_date: Any,
    end_date: Any,
    *,
    now: datetime | None = None,
) -> bool:
    """Return ``True`` when requested window overlaps the current month."""

    start_ts = _to_timestamp(start_date)
    end_ts = _to_timestamp(end_date)
    if end_ts < start_ts:
        start_ts, end_ts = end_ts, start_ts
    now_ts = _to_timestamp(now or datetime.now(UTC))
    month_start = now_ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    month_end = month_start + pd.offsets.MonthEnd(1)
    return bool(start_ts <= month_end and end_ts >= month_start)


def _is_expired(path: Path, ttl_hours: int | None) -> bool:
    """Return ``True`` when cache file age exceeds configured TTL."""

    if ttl_hours is None:
        return False
    age_hours = (time.time() - path.stat().st_mtime) / 3600.0
    return age_hours > ttl_hours


def _safe_read(path: Path) -> pd.Series | None:
    """Read a cached parquet series; delete file and return ``None`` on corruption."""

    try:
        frame = pd.read_parquet(path)
    except (EmptyDataError, ParserError, OSError, ValueError):
        path.unlink(missing_ok=True)
        return None

    if frame.empty:
        return pd.Series(dtype=float)
    if "value" not in frame.columns:
        path.unlink(missing_ok=True)
        return None

    series = frame["value"]
    if not isinstance(series.index, pd.DatetimeIndex):
        series.index = pd.to_datetime(series.index, errors="coerce")
    series = series[~series.index.isna()]
    return series.astype(float)


def cache_key(
    *,
    symbol: str,
    instrument_type: str,
    what_to_show: str,
    bar_size: str,
    use_rth: bool,
    start_date: Any,
    end_date: Any,
) -> str:
    """Build deterministic cache key fingerprint for an IBKR request."""
    start_iso = _to_timestamp(start_date).date().isoformat()
    end_iso = _to_timestamp(end_date).date().isoformat()
    key = "|".join(
        [
            str(symbol or "").strip().upper(),
            str(instrument_type or "").strip().lower(),
            str(what_to_show or "").strip().upper(),
            str(bar_size or "").strip(),
            "rth" if bool(use_rth) else "all",
            start_iso,
            end_iso,
        ]
    )
    return hashlib.md5(key.encode()).hexdigest()


def _cache_path(
    *,
    symbol: str,
    instrument_type: str,
    what_to_show: str,
    bar_size: str,
    use_rth: bool,
    start_date: Any,
    end_date: Any,
    base_dir: str | Path | None = None,
) -> Path:
    """Build on-disk cache path for the deterministic request key."""

    key = cache_key(
        symbol=symbol,
        instrument_type=instrument_type,
        what_to_show=what_to_show,
        bar_size=bar_size,
        use_rth=use_rth,
        start_date=start_date,
        end_date=end_date,
    )
    return _cache_dir(base_dir) / f"ibkr_{key}.parquet"


def get_cached(
    *,
    symbol: str,
    instrument_type: str,
    what_to_show: str,
    bar_size: str,
    use_rth: bool,
    start_date: Any,
    end_date: Any,
    base_dir: str | Path | None = None,
    now: datetime | None = None,
) -> pd.Series | None:
    """Read cached series when present/fresh, else return ``None``.

    Routing semantics:
    - Current-month windows enforce ``CURRENT_MONTH_TTL_HOURS``.
    - Historical windows have no TTL and rely on explicit eviction.
    """
    path = _cache_path(
        symbol=symbol,
        instrument_type=instrument_type,
        what_to_show=what_to_show,
        bar_size=bar_size,
        use_rth=use_rth,
        start_date=start_date,
        end_date=end_date,
        base_dir=base_dir,
    )
    if not path.is_file():
        return None

    ttl = CURRENT_MONTH_TTL_HOURS if _includes_current_month(start_date, end_date, now=now) else None
    if _is_expired(path, ttl):
        path.unlink(missing_ok=True)
        return None

    series = _safe_read(path)
    if series is None:
        return None
    series.name = str(symbol or "").strip().upper()
    return series


def put_cache(
    series: pd.Series,
    *,
    symbol: str,
    instrument_type: str,
    what_to_show: str,
    bar_size: str,
    use_rth: bool,
    start_date: Any,
    end_date: Any,
    base_dir: str | Path | None = None,
) -> Path | None:
    """Persist non-empty series in parquet format and return cache file path."""
    if series.empty or series.dropna().empty:
        return None

    cleaned = series.dropna().astype(float).copy()
    if not isinstance(cleaned.index, pd.DatetimeIndex):
        cleaned.index = pd.to_datetime(cleaned.index, errors="coerce")
    cleaned = cleaned[~cleaned.index.isna()]
    if cleaned.empty:
        return None

    path = _cache_path(
        symbol=symbol,
        instrument_type=instrument_type,
        what_to_show=what_to_show,
        bar_size=bar_size,
        use_rth=use_rth,
        start_date=start_date,
        end_date=end_date,
        base_dir=base_dir,
    )
    frame = cleaned.to_frame(name="value")
    frame.to_parquet(path, engine="pyarrow", compression="zstd", index=True)
    return path


def disk_cache_stats(base_dir: str | Path | None = None) -> Dict[str, Any]:
    """Return cache file-count/size/age summary for diagnostics surfaces."""
    cache_path = _cache_dir(base_dir)
    files = list(cache_path.glob("ibkr_*.parquet"))

    if not files:
        return {
            "file_count": 0,
            "total_bytes": 0,
            "total_mb": 0.0,
            "oldest": None,
            "newest": None,
            "cache_enabled": True,
        }

    total_bytes = 0
    mtimes: list[float] = []
    for f in files:
        try:
            st = f.stat()
            total_bytes += st.st_size
            mtimes.append(st.st_mtime)
        except OSError:
            continue

    if not mtimes:
        return {
            "file_count": 0,
            "total_bytes": 0,
            "total_mb": 0.0,
            "oldest": None,
            "newest": None,
            "cache_enabled": True,
        }

    return {
        "file_count": len(mtimes),
        "total_bytes": total_bytes,
        "total_mb": round(total_bytes / (1024 * 1024), 2),
        "oldest": datetime.fromtimestamp(min(mtimes), tz=UTC).isoformat(),
        "newest": datetime.fromtimestamp(max(mtimes), tz=UTC).isoformat(),
        "cache_enabled": True,
    }


def clear_disk_cache(
    base_dir: str | Path | None = None,
    older_than_hours: int | None = None,
) -> Dict[str, Any]:
    """Remove IBKR parquet cache files, optionally filtered by age.

    Args:
        base_dir: Override project root for cache directory.
        older_than_hours: If set, only remove files older than this many hours.
            Must be >= 0.

    Returns:
        Summary dict with ``files_removed``, ``bytes_freed``, ``mb_freed``,
        and optional ``errors``.
    """
    if older_than_hours is not None and older_than_hours < 0:
        raise ValueError(f"older_than_hours must be >= 0, got {older_than_hours}")

    cache_path = _cache_dir(base_dir)
    files = list(cache_path.glob("ibkr_*.parquet"))
    removed = 0
    freed = 0
    errors: list[str] = []

    for f in files:
        try:
            st = f.stat()
            if older_than_hours is not None:
                age_h = (time.time() - st.st_mtime) / 3600.0
                if age_h <= older_than_hours:
                    continue
            freed += st.st_size
            f.unlink(missing_ok=True)
            removed += 1
        except OSError as e:
            errors.append(f"{f.name}: {e}")

    result: Dict[str, Any] = {
        "files_removed": removed,
        "bytes_freed": freed,
        "mb_freed": round(freed / (1024 * 1024), 2),
    }
    if errors:
        result["errors"] = errors
    return result
