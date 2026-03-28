"""
SumoSim Configuration and Constants
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "cache"
CONFIG_PATH = PROJECT_ROOT / "config.yaml"

MAKUUCHI_SIZE = 42
TOURNAMENT_DAYS = 15
KACHI_KOSHI_THRESHOLD = 8
NUM_BASHO_PER_YEAR = 6

RANK_PRIORITY = ["yokozuna", "ozeki", "sekiwake", "komusubi", "maegashira"]

DEFAULT_RANK_RATINGS: dict[str, float] = {
    "yokozuna": 1800.0,
    "ozeki": 1700.0,
    "sekiwake": 1600.0,
    "komusubi": 1550.0,
    "maegashira": 1500.0,
}

MAEGASHIRA_RATING_STEP = 10.0


@dataclass
class SimulationConfig:
    # Monte Carlo
    bout_iterations: int = 10_000
    tournament_iterations: int = 1_000
    noise_sigma: float = 0.15

    # Rating calculation
    recency_decay: float = 0.7
    recency_basho_count: int = 3
    logistic_k: float = 0.004
    head_to_head_weight: float = 0.15

    # Modifier defaults
    momentum_weight: float = 0.5
    momentum_max_adjustment: float = 150.0
    momentum_streak_window: int = 5
    matchup_weight: float = 0.3
    matchup_max_adjustment: float = 100.0
    injury_max_adjustment: float = 200.0
    fatigue_max_adjustment: float = 100.0
    default_recovery_factor: float = 0.6

    # Data scraping
    cache_ttl_hours: int = 24
    scrape_delay_ms: int = 1500

    # Random seed
    random_seed: Optional[int] = None


_default_config: Optional[SimulationConfig] = None


def get_config() -> SimulationConfig:
    global _default_config
    if _default_config is None:
        _default_config = SimulationConfig()
    return _default_config


def set_config(config: SimulationConfig) -> None:
    global _default_config
    _default_config = config
