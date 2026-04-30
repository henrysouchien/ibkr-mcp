"""Shared incremental time-series cache utilities."""

from __future__ import annotations

import json
import os
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Callable, Iterable

import pandas as pd
from pandas.errors import EmptyDataError, ParserError


def _safe_load(path: Path) -> pd.DataFrame | None:
    """Safely load a parquet file, removing corrupted files."""
    try:
        return pd.read_parquet(path)
    except (EmptyDataError, ParserError, OSError, ValueError) as e:
        print(f"Cache file corrupted, deleting: {path.name} ({type(e).__name__})")
        path.unlink(missing_ok=True)
        return None


def _is_expired(path: Path, ttl_hours: int | None) -> bool:
    """Check if a cached file has expired based on TTL."""
    if ttl_hours is None:
        return False
    age_hours = (time.time() - path.stat().st_mtime) / 3600
    return age_hours > ttl_hours


def _atomic_write_parquet(df: pd.DataFrame, path: Path) -> None:
    """Write parquet atomically via temporary file + replace."""
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        df.to_parquet(tmp_path, engine="pyarrow", compression="zstd", index=True)
        os.replace(tmp_path, path)
    finally:
        tmp_path.unlink(missing_ok=True)


def _atomic_write_text(contents: str, path: Path) -> None:
    """Write text atomically via temporary file + replace."""
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        tmp_path.write_text(contents, encoding="utf-8")
        os.replace(tmp_path, path)
    finally:
        tmp_path.unlink(missing_ok=True)


def _coerce_date_bound(value: str | None) -> pd.Timestamp | None:
    """Normalize optional date bounds to midnight timestamps."""
    if value is None:
        return None
    ts = pd.Timestamp(pd.to_datetime(value))
    if ts.tzinfo is not None:
        ts = ts.tz_localize(None)
    return ts.normalize()


def _date_str(value: pd.Timestamp | None) -> str | None:
    """Serialize optional timestamps back to YYYY-MM-DD strings."""
    if value is None:
        return None
    return value.date().isoformat()


def _coerce_month_end_bound(value: str | None) -> pd.Timestamp | None:
    """Normalize optional date bounds to the containing month-end label."""
    ts = _coerce_date_bound(value)
    if ts is None:
        return None
    return (ts + pd.offsets.MonthEnd(0)).normalize()


def _normalize_series(series: pd.Series) -> pd.Series:
    """Normalize time series indexes for deterministic parquet storage."""
    if not isinstance(series, pd.Series):
        raise TypeError(f"Expected pandas Series, got {type(series).__name__}")

    normalized = series.copy()
    index = pd.DatetimeIndex(pd.to_datetime(normalized.index, errors="coerce"))
    if getattr(index, "tz", None) is not None:
        index = index.tz_localize(None)
    valid_mask = ~index.isna()
    if not bool(valid_mask.all()):
        normalized = normalized.loc[valid_mask].copy()
        index = index[valid_mask]
    if len(index) == 0:
        return normalized.iloc[0:0]

    normalized.index = index.normalize()
    if not normalized.index.is_monotonic_increasing:
        normalized = normalized.sort_index()
    if normalized.index.has_duplicates:
        normalized = normalized.loc[~normalized.index.duplicated(keep="last")]
    return normalized


def _is_empty_loader_error(exc: Exception) -> bool:
    """Detect boundary extension misses that mean "no rows for this date slice"."""
    if type(exc).__name__ == "FMPEmptyResponseError":
        return True
    if isinstance(exc, ValueError):
        message = str(exc).lower()
        return "no data found" in message or "empty data" in message
    return False


def _merge_series(parts: Iterable[pd.Series]) -> pd.Series:
    """Merge multiple overlapping series into a single sorted daily series."""
    prepared = [_normalize_series(part) for part in parts if isinstance(part, pd.Series) and not part.empty]
    if not prepared:
        return pd.Series(dtype=float)
    merged = pd.concat(prepared).sort_index()
    if merged.index.has_duplicates:
        merged = merged.loc[~merged.index.duplicated(keep="last")]
    return merged


_MISSING_COVERAGE_MTIME = -1


class TimeSeriesStore:
    """Append-only daily time series cache keyed by (ticker, series_kind)."""

    CACHE_SUBDIR = Path("cache") / "timeseries"

    def __init__(self, base_dir: str | Path):
        self.base_dir = Path(base_dir).expanduser().resolve()
        self.cache_dir = self.base_dir / self.CACHE_SUBDIR
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._locks: dict[str, threading.Lock] = {}
        self._lock_guard = threading.Lock()
        self._series_cache: dict[str, tuple[int, pd.Series]] = {}
        self._coverage_cache: dict[str, tuple[int, tuple[pd.Timestamp | None, pd.Timestamp | None]]] = {}

    def _get_path(self, ticker: str, series_kind: str) -> Path:
        return self.cache_dir / f"{ticker}_{series_kind}.parquet"

    def _get_meta_path(self, path: Path) -> Path:
        return path.with_suffix(".coverage.json")

    def _clear_coverage(self, path: Path) -> None:
        meta_path = self._get_meta_path(path)
        cache_key = self._cache_key(meta_path)
        cached_entry = self._coverage_cache.get(cache_key)
        if cached_entry is not None and cached_entry[0] == _MISSING_COVERAGE_MTIME:
            return
        meta_path.unlink(missing_ok=True)
        self._coverage_cache[cache_key] = (_MISSING_COVERAGE_MTIME, (None, None))

    def _get_lock(self, path: Path) -> threading.Lock:
        key = str(path)
        with self._lock_guard:
            lock = self._locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._locks[key] = lock
            return lock

    @staticmethod
    def _cache_key(path: Path) -> str:
        return str(path)

    def _load_series(self, path: Path) -> pd.Series | None:
        cache_key = self._cache_key(path)
        try:
            mtime_ns = path.stat().st_mtime_ns
        except OSError:
            self._series_cache.pop(cache_key, None)
            self._clear_coverage(path)
            return None

        cached_entry = self._series_cache.get(cache_key)
        if cached_entry is not None and cached_entry[0] == mtime_ns:
            return cached_entry[1].copy()

        df = _safe_load(path)
        if df is None:
            self._clear_coverage(path)
            self._series_cache.pop(cache_key, None)
            return None
        if df.empty or len(df.columns) == 0:
            path.unlink(missing_ok=True)
            self._clear_coverage(path)
            self._series_cache.pop(cache_key, None)
            return None
        column = "value" if "value" in df.columns else df.columns[0]
        normalized = _normalize_series(df[column])
        self._series_cache[cache_key] = (mtime_ns, normalized)
        return normalized.copy()

    def _write_series(self, path: Path, series: pd.Series) -> None:
        prepared = _normalize_series(series)
        if prepared.empty:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write_parquet(prepared.to_frame(name="value"), path)
        try:
            self._series_cache[self._cache_key(path)] = (path.stat().st_mtime_ns, prepared)
        except OSError:
            self._series_cache.pop(self._cache_key(path), None)

    def _load_coverage(self, path: Path) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
        meta_path = self._get_meta_path(path)
        cache_key = self._cache_key(meta_path)
        cached_entry = self._coverage_cache.get(cache_key)
        if cached_entry is not None and cached_entry[0] == _MISSING_COVERAGE_MTIME:
            return cached_entry[1]
        if not meta_path.is_file():
            self._coverage_cache[cache_key] = (_MISSING_COVERAGE_MTIME, (None, None))
            return None, None
        try:
            mtime_ns = meta_path.stat().st_mtime_ns
        except OSError:
            self._coverage_cache[cache_key] = (_MISSING_COVERAGE_MTIME, (None, None))
            return None, None

        if cached_entry is not None and cached_entry[0] == mtime_ns:
            return cached_entry[1]
        try:
            payload = json.loads(meta_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            meta_path.unlink(missing_ok=True)
            self._coverage_cache[cache_key] = (_MISSING_COVERAGE_MTIME, (None, None))
            return None, None
        if not isinstance(payload, dict):
            meta_path.unlink(missing_ok=True)
            self._coverage_cache[cache_key] = (_MISSING_COVERAGE_MTIME, (None, None))
            return None, None
        coverage = (
            _coerce_date_bound(payload.get("requested_start")),
            _coerce_date_bound(payload.get("requested_end")),
        )
        self._coverage_cache[cache_key] = (mtime_ns, coverage)
        return coverage

    def _write_coverage(
        self,
        path: Path,
        *,
        requested_start: pd.Timestamp | None,
        requested_end: pd.Timestamp | None,
    ) -> None:
        meta_path = self._get_meta_path(path)
        meta_path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(
            {
                "requested_start": _date_str(requested_start),
                "requested_end": _date_str(requested_end),
            },
            sort_keys=True,
        )
        _atomic_write_text(payload, meta_path)
        try:
            self._coverage_cache[self._cache_key(meta_path)] = (
                meta_path.stat().st_mtime_ns,
                (requested_start, requested_end),
            )
        except OSError:
            self._coverage_cache.pop(self._cache_key(meta_path), None)

    def _sync_coverage(
        self,
        path: Path,
        *,
        series: pd.Series,
        requested_start: pd.Timestamp | None,
        requested_end: pd.Timestamp | None,
    ) -> None:
        """Persist sidecar coverage only when requested bounds exceed stored row bounds."""

        if series.empty:
            self._clear_coverage(path)
            return

        actual_start = series.index.min()
        actual_end = series.index.max()
        effective_start = requested_start or actual_start
        effective_end = requested_end or actual_end

        if effective_start == actual_start and effective_end == actual_end:
            self._clear_coverage(path)
            return

        self._write_coverage(
            path,
            requested_start=effective_start,
            requested_end=effective_end,
        )

    @staticmethod
    def _is_stale(path: Path, max_age_days: int | None) -> bool:
        if max_age_days is None:
            return False
        age_seconds = time.time() - path.stat().st_mtime
        return age_seconds > max(0, int(max_age_days)) * 24 * 60 * 60

    @staticmethod
    def _slice(series: pd.Series, start: pd.Timestamp | None, end: pd.Timestamp | None) -> pd.Series:
        if series.empty:
            return series
        if start is not None and end is not None:
            return series.loc[start:end]
        if start is not None:
            return series.loc[start:]
        if end is not None:
            return series.loc[:end]
        return series

    def read(
        self,
        ticker: str,
        series_kind: str,
        start: str | None,
        end: str | None,
        loader: Callable[[str | None, str | None], pd.Series],
        *,
        resample: str | None = None,
        max_age_days: int | None = None,
    ) -> pd.Series:
        """
        Return cached daily series sliced to [start, end], extending via loader if needed.
        """
        path = self._get_path(ticker, series_kind)
        start_ts = _coerce_date_bound(start)
        end_ts = _coerce_date_bound(end)

        with self._get_lock(path):
            cached = self._load_series(path) if path.is_file() else None
            coverage_start, coverage_end = self._load_coverage(path)

            if cached is not None and self._is_stale(path, max_age_days):
                refresh_start = coverage_start or cached.index.min()
                refresh_end: pd.Timestamp | None = None
                if start_ts is not None and start_ts < refresh_start:
                    refresh_start = start_ts
                if end_ts is not None:
                    refresh_end = max(coverage_end or cached.index.max(), end_ts)

                refreshed = _normalize_series(loader(_date_str(refresh_start), _date_str(refresh_end)))
                if not refreshed.empty:
                    cached = refreshed
                    self._write_series(path, cached)
                    self._sync_coverage(
                        path,
                        series=cached,
                        requested_start=start_ts or refresh_start,
                        requested_end=end_ts or refresh_end or cached.index.max(),
                    )

            if cached is None:
                cached = _normalize_series(loader(start, end))
                if not cached.empty:
                    self._write_series(path, cached)
                    self._sync_coverage(
                        path,
                        series=cached,
                        requested_start=start_ts or cached.index.min(),
                        requested_end=end_ts or cached.index.max(),
                    )
            elif not cached.empty:
                pieces = [cached]
                cache_min = cached.index.min()
                cache_max = cached.index.max()
                covered_start = coverage_start or cache_min
                covered_end = coverage_end or cache_max
                expanded_coverage = False

                if start_ts is not None and start_ts < covered_start:
                    prefix_end = cache_min - pd.Timedelta(days=1)
                    try:
                        prefix = _normalize_series(loader(_date_str(start_ts), _date_str(prefix_end)))
                    except Exception as exc:
                        if _is_empty_loader_error(exc):
                            prefix = pd.Series(dtype=float)
                        else:
                            raise
                    if not prefix.empty:
                        pieces.append(prefix)
                    coverage_start = start_ts
                    expanded_coverage = True

                if end_ts is not None and end_ts > covered_end:
                    suffix_start = cache_max + pd.Timedelta(days=1)
                    try:
                        suffix = _normalize_series(loader(_date_str(suffix_start), _date_str(end_ts)))
                    except Exception as exc:
                        if _is_empty_loader_error(exc):
                            suffix = pd.Series(dtype=float)
                        else:
                            raise
                    if not suffix.empty:
                        pieces.append(suffix)
                    coverage_end = end_ts
                    expanded_coverage = True

                if len(pieces) > 1:
                    cached = _merge_series(pieces)
                    self._write_series(path, cached)
                if expanded_coverage or coverage_start is None or coverage_end is None:
                    self._sync_coverage(
                        path,
                        series=cached,
                        requested_start=coverage_start or cache_min,
                        requested_end=coverage_end or cache_max,
                    )

            result = cached if cached is not None else pd.Series(dtype=float)
            result = self._slice(result, start_ts, end_ts)
            if resample is not None and not result.empty:
                result = result.resample(resample).last()
            return result.copy()

    def read_monthly(
        self,
        ticker: str,
        series_kind: str,
        start: str | None,
        end: str | None,
        loader: Callable[[str | None, str | None], pd.Series],
        *,
        max_age_days: int | None = None,
    ) -> pd.Series:
        """
        Return cached month-end series sliced to the containing month labels.

        Unlike ``read()``, this treats requested coverage in month-end label space.
        That preserves monthly-analysis semantics without forcing day-level slicing
        on series indexed by month-end timestamps.
        """
        path = self._get_path(ticker, series_kind)
        start_label = _coerce_month_end_bound(start)
        end_label = _coerce_month_end_bound(end)

        with self._get_lock(path):
            cached = self._load_series(path) if path.is_file() else None
            coverage_start, coverage_end = self._load_coverage(path)

            if cached is not None and self._is_stale(path, max_age_days):
                refresh_start = coverage_start or cached.index.min()
                refresh_end: pd.Timestamp | None = None
                if start_label is not None and start_label < refresh_start:
                    refresh_start = start_label
                if end_label is not None:
                    refresh_end = max(coverage_end or cached.index.max(), end_label)

                refreshed = _normalize_series(loader(_date_str(refresh_start), _date_str(refresh_end)))
                if not refreshed.empty:
                    cached = refreshed
                    self._write_series(path, cached)
                    self._sync_coverage(
                        path,
                        series=cached,
                        requested_start=start_label or refresh_start,
                        requested_end=end_label or refresh_end or cached.index.max(),
                    )

            if cached is None:
                cached = _normalize_series(loader(_date_str(start_label), _date_str(end_label)))
                if not cached.empty:
                    self._write_series(path, cached)
                    self._sync_coverage(
                        path,
                        series=cached,
                        requested_start=start_label or cached.index.min(),
                        requested_end=end_label or cached.index.max(),
                    )
            elif not cached.empty:
                pieces = [cached]
                cache_min = cached.index.min()
                cache_max = cached.index.max()
                covered_start = coverage_start or cache_min
                covered_end = coverage_end or cache_max
                expanded_coverage = False

                if start_label is not None and start_label < covered_start:
                    prefix_end = (covered_start - pd.offsets.MonthEnd(1)).normalize()
                    try:
                        prefix = _normalize_series(
                            loader(_date_str(start_label), _date_str(prefix_end))
                        )
                    except Exception as exc:
                        if _is_empty_loader_error(exc):
                            prefix = pd.Series(dtype=float)
                        else:
                            raise
                    if not prefix.empty:
                        pieces.append(prefix)
                    coverage_start = start_label
                    expanded_coverage = True

                if end_label is not None and end_label > covered_end:
                    suffix_start = (covered_end + pd.offsets.MonthEnd(1)).normalize()
                    try:
                        suffix = _normalize_series(
                            loader(_date_str(suffix_start), _date_str(end_label))
                        )
                    except Exception as exc:
                        if _is_empty_loader_error(exc):
                            suffix = pd.Series(dtype=float)
                        else:
                            raise
                    if not suffix.empty:
                        pieces.append(suffix)
                    coverage_end = end_label
                    expanded_coverage = True

                if len(pieces) > 1:
                    cached = _merge_series(pieces)
                    self._write_series(path, cached)
                if expanded_coverage or coverage_start is None or coverage_end is None:
                    self._sync_coverage(
                        path,
                        series=cached,
                        requested_start=coverage_start or cache_min,
                        requested_end=coverage_end or cache_max,
                    )

            result = cached if cached is not None else pd.Series(dtype=float)
            result = self._slice(result, start_label, end_label)
            return result.copy()

    def clear(self, series_kind: str | None = None) -> int:
        """Delete cached time series files, optionally filtered by series kind."""
        pattern = "*.parquet" if series_kind is None else f"*_{series_kind}.parquet"
        removed = 0
        for path in list(self.cache_dir.glob(pattern)):
            with self._get_lock(path):
                if path.exists():
                    path.unlink(missing_ok=True)
                    self._get_meta_path(path).unlink(missing_ok=True)
                    self._series_cache.pop(self._cache_key(path), None)
                    self._coverage_cache.pop(self._cache_key(self._get_meta_path(path)), None)
                    removed += 1
        return removed

    def stats(self, series_kind: str | None = None) -> dict[str, Any]:
        """Return file-count, size, and date-coverage metadata for the store."""
        pattern = "*.parquet" if series_kind is None else f"*_{series_kind}.parquet"
        total_bytes = 0
        coverage: dict[str, dict[str, Any]] = {}

        for path in sorted(self.cache_dir.glob(pattern)):
            if not path.is_file():
                continue
            series = self._load_series(path)
            if series is None:
                continue
            try:
                size_bytes = path.stat().st_size
            except OSError:
                size_bytes = 0
            total_bytes += size_bytes
            coverage[path.stem] = {
                "start": _date_str(series.index.min()) if not series.empty else None,
                "end": _date_str(series.index.max()) if not series.empty else None,
                "rows": int(len(series)),
                "bytes": int(size_bytes),
            }

        return {
            "series_kind": series_kind or "all",
            "file_count": len(coverage),
            "total_bytes": int(total_bytes),
            "total_mb": round(total_bytes / (1024 * 1024), 4),
            "date_coverage": coverage,
        }
