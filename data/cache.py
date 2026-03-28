"""
SumoSim Cache Manager

Local JSON file cache organized by basho with configurable TTL.
Enables full offline operation after initial data pull.

Directory structure:
    cache/
        wrestlers.json          # Full wrestler roster
        wrestlers_meta.json     # Timestamp metadata
        202501/                 # Per-basho directory
            banzuke.json
            results_day01.json
            results_day02.json
            ...
            tournament_records.json
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, is_dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Optional

from utils.config import get_config, DATA_DIR

logger = logging.getLogger(__name__)


class CacheTTL(Enum):
    """Cache freshness categories with TTL in hours."""
    WRESTLER_PROFILES = 168    # 1 week
    ACTIVE_BOUT_RESULTS = 4   # 4 hours during active basho
    HISTORICAL_RESULTS = 0    # Never expires (0 = indefinite)
    BANZUKE = 168              # 1 week
    TOURNAMENT_RECORDS = 24    # 1 day


class CacheManager:
    """
    Manages local JSON file cache for scraped sumo data.

    Usage:
        cache = CacheManager()
        cache.save_wrestlers(wrestlers_data)
        wrestlers = cache.load_wrestlers()

        cache.save_banzuke("202501", banzuke_data)
        banzuke = cache.load_banzuke("202501")
    """

    def __init__(self, cache_dir: Path | None = None):
        self._cache_dir = cache_dir or DATA_DIR
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    @property
    def cache_dir(self) -> Path:
        return self._cache_dir

    # ------------------------------------------------------------------
    # Wrestler roster
    # ------------------------------------------------------------------

    def save_wrestlers(self, data: list[dict]) -> Path:
        """Save the full wrestler roster."""
        path = self._cache_dir / "wrestlers.json"
        self._write_json(path, data)
        self._write_meta(self._cache_dir / "wrestlers_meta.json")
        logger.info(f"Cached {len(data)} wrestler profiles")
        return path

    def load_wrestlers(self) -> Optional[list[dict]]:
        """Load cached wrestler roster, or None if stale/missing."""
        path = self._cache_dir / "wrestlers.json"
        meta_path = self._cache_dir / "wrestlers_meta.json"
        if not self._is_fresh(meta_path, CacheTTL.WRESTLER_PROFILES):
            return None
        return self._read_json(path)

    # ------------------------------------------------------------------
    # Banzuke (per basho)
    # ------------------------------------------------------------------

    def save_banzuke(self, basho_id: str, data: list[dict]) -> Path:
        """Save banzuke for a specific tournament."""
        basho_dir = self._basho_dir(basho_id)
        path = basho_dir / "banzuke.json"
        self._write_json(path, data)
        self._write_meta(basho_dir / "banzuke_meta.json")
        logger.info(f"Cached banzuke for {basho_id}: {len(data)} entries")
        return path

    def load_banzuke(self, basho_id: str) -> Optional[list[dict]]:
        """Load cached banzuke, or None if stale/missing."""
        basho_dir = self._basho_dir(basho_id)
        meta_path = basho_dir / "banzuke_meta.json"
        if not self._is_fresh(meta_path, CacheTTL.BANZUKE):
            return None
        return self._read_json(basho_dir / "banzuke.json")

    # ------------------------------------------------------------------
    # Daily results (per basho, per day)
    # ------------------------------------------------------------------

    def save_day_results(
        self, basho_id: str, day: int, data: list[dict], is_historical: bool = False
    ) -> Path:
        """Save bout results for a specific day."""
        basho_dir = self._basho_dir(basho_id)
        path = basho_dir / f"results_day{day:02d}.json"
        self._write_json(path, data)

        meta = {"timestamp": time.time(), "is_historical": is_historical}
        self._write_json(basho_dir / f"results_day{day:02d}_meta.json", meta)
        logger.info(f"Cached {len(data)} bouts for {basho_id} day {day}")
        return path

    def load_day_results(
        self, basho_id: str, day: int, is_active_basho: bool = False
    ) -> Optional[list[dict]]:
        """Load cached day results, or None if stale/missing."""
        basho_dir = self._basho_dir(basho_id)
        meta_path = basho_dir / f"results_day{day:02d}_meta.json"

        meta = self._read_json(meta_path)
        if meta is None:
            return None

        # Historical results never expire
        is_historical = meta.get("is_historical", False)
        if is_historical:
            ttl = CacheTTL.HISTORICAL_RESULTS
        elif is_active_basho:
            ttl = CacheTTL.ACTIVE_BOUT_RESULTS
        else:
            ttl = CacheTTL.HISTORICAL_RESULTS

        if not self._is_fresh(meta_path, ttl):
            return None

        return self._read_json(basho_dir / f"results_day{day:02d}.json")

    # ------------------------------------------------------------------
    # Tournament records (aggregated per basho)
    # ------------------------------------------------------------------

    def save_tournament_records(self, basho_id: str, data: list[dict]) -> Path:
        """Save tournament records (final standings) for a basho."""
        basho_dir = self._basho_dir(basho_id)
        path = basho_dir / "tournament_records.json"
        self._write_json(path, data)
        self._write_meta(basho_dir / "tournament_records_meta.json")
        logger.info(f"Cached {len(data)} tournament records for {basho_id}")
        return path

    def load_tournament_records(self, basho_id: str) -> Optional[list[dict]]:
        """Load cached tournament records."""
        basho_dir = self._basho_dir(basho_id)
        meta_path = basho_dir / "tournament_records_meta.json"
        if not self._is_fresh(meta_path, CacheTTL.TOURNAMENT_RECORDS):
            return None
        return self._read_json(basho_dir / "tournament_records.json")

    # ------------------------------------------------------------------
    # Head-to-head cache
    # ------------------------------------------------------------------

    def save_head_to_head(
        self, wrestler_a: str, wrestler_b: str, data: list[dict]
    ) -> Path:
        """Cache head-to-head bout history between two wrestlers."""
        h2h_dir = self._cache_dir / "h2h"
        h2h_dir.mkdir(exist_ok=True)
        # Canonical key: sorted IDs to avoid duplicates
        key = "_vs_".join(sorted([wrestler_a, wrestler_b]))
        path = h2h_dir / f"{key}.json"
        self._write_json(path, data)
        return path

    def load_head_to_head(
        self, wrestler_a: str, wrestler_b: str
    ) -> Optional[list[dict]]:
        """Load cached head-to-head history."""
        h2h_dir = self._cache_dir / "h2h"
        key = "_vs_".join(sorted([wrestler_a, wrestler_b]))
        path = h2h_dir / f"{key}.json"
        return self._read_json(path)

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def list_cached_basho(self) -> list[str]:
        """Return a list of basho IDs that have cached data."""
        result = []
        if not self._cache_dir.exists():
            return result
        for d in sorted(self._cache_dir.iterdir()):
            if d.is_dir() and len(d.name) == 6 and d.name.isdigit():
                # Convert directory name 202501 -> 2025.01
                basho_id = f"{d.name[:4]}.{d.name[4:]}"
                result.append(basho_id)
        return result

    def get_cache_age_hours(self, basho_id: str, data_type: str) -> Optional[float]:
        """Return age of cached data in hours, or None if not cached."""
        basho_dir = self._basho_dir(basho_id)
        meta_path = basho_dir / f"{data_type}_meta.json"
        meta = self._read_json(meta_path)
        if meta is None:
            return None
        ts = meta.get("timestamp", 0)
        return (time.time() - ts) / 3600.0

    def clear_basho(self, basho_id: str) -> None:
        """Remove all cached data for a specific basho."""
        basho_dir = self._basho_dir(basho_id)
        if basho_dir.exists():
            import shutil
            shutil.rmtree(basho_dir)
            logger.info(f"Cleared cache for {basho_id}")

    def clear_all(self) -> None:
        """Remove all cached data."""
        import shutil
        if self._cache_dir.exists():
            shutil.rmtree(self._cache_dir)
            self._cache_dir.mkdir(parents=True, exist_ok=True)
            logger.info("Cleared entire cache")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _basho_dir(self, basho_id: str) -> Path:
        """Get or create the directory for a specific basho.
        basho_id format: '2025.01' -> directory '202501'
        """
        dir_name = basho_id.replace(".", "")
        basho_dir = self._cache_dir / dir_name
        basho_dir.mkdir(parents=True, exist_ok=True)
        return basho_dir

    def _write_json(self, path: Path, data: Any) -> None:
        """Write data to JSON file with pretty printing."""
        serializable = self._make_serializable(data)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(serializable, f, indent=2, ensure_ascii=False)

    def _read_json(self, path: Path) -> Optional[Any]:
        """Read JSON file, return None if missing or corrupt."""
        if not path.exists():
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Failed to read cache file {path}: {e}")
            return None

    def _write_meta(self, path: Path) -> None:
        """Write a timestamp metadata file."""
        self._write_json(path, {"timestamp": time.time()})

    def _is_fresh(self, meta_path: Path, ttl: CacheTTL) -> bool:
        """Check if cached data is still fresh based on TTL."""
        if ttl == CacheTTL.HISTORICAL_RESULTS:
            # Never expires — just check existence
            data_path = meta_path.parent / meta_path.name.replace("_meta", "")
            return data_path.exists()

        meta = self._read_json(meta_path)
        if meta is None:
            return False

        ts = meta.get("timestamp", 0)
        age_hours = (time.time() - ts) / 3600.0
        return age_hours < ttl.value

    @staticmethod
    def _make_serializable(obj: Any) -> Any:
        """Convert dataclasses and enums to JSON-serializable form."""
        if is_dataclass(obj) and not isinstance(obj, type):
            return {k: CacheManager._make_serializable(v) for k, v in asdict(obj).items()}
        elif isinstance(obj, Enum):
            return obj.value
        elif isinstance(obj, (list, tuple)):
            return [CacheManager._make_serializable(item) for item in obj]
        elif isinstance(obj, dict):
            return {k: CacheManager._make_serializable(v) for k, v in obj.items()}
        return obj
