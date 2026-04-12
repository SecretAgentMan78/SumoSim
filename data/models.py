"""
SumoSim Data Models

Frozen dataclasses with validation. Drop-in replaceable with Pydantic
when available -- field names, types, and semantics are identical.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Optional

_BASHO_RE = re.compile(r"^\d{4}\.\d{2}$")


class Rank(str, Enum):
    YOKOZUNA = "yokozuna"
    OZEKI = "ozeki"
    SEKIWAKE = "sekiwake"
    KOMUSUBI = "komusubi"
    MAEGASHIRA = "maegashira"
    JURYO = "juryo"
    MAKUSHITA = "makushita"
    SANDANME = "sandanme"
    JONIDAN = "jonidan"
    JONOKUCHI = "jonokuchi"

    @property
    def tier(self) -> int:
        return {
            Rank.YOKOZUNA: 1, Rank.OZEKI: 2, Rank.SEKIWAKE: 3,
            Rank.KOMUSUBI: 4, Rank.MAEGASHIRA: 5,
            Rank.JURYO: 6, Rank.MAKUSHITA: 7, Rank.SANDANME: 8,
            Rank.JONIDAN: 9, Rank.JONOKUCHI: 10,
        }.get(self, 11)


class Division(str, Enum):
    MAKUUCHI = "makuuchi"
    JURYO = "juryo"


class FightingStyle(str, Enum):
    OSHI = "oshi"
    YOTSU = "yotsu"
    HYBRID = "hybrid"


class MomentumState(str, Enum):
    HOT = "hot"
    WARM = "warm"
    NEUTRAL = "neutral"
    COOL = "cool"
    COLD = "cold"


class Basho(str, Enum):
    HATSU = "01"
    HARU = "03"
    NATSU = "05"
    NAGOYA = "07"
    AKI = "09"
    KYUSHU = "11"

    @property
    def display_name(self) -> str:
        return {
            "01": "Hatsu (January)", "03": "Haru (March)",
            "05": "Natsu (May)", "07": "Nagoya (July)",
            "09": "Aki (September)", "11": "Kyushu (November)",
        }[self.value]

    @property
    def city(self) -> str:
        return {
            "01": "Tokyo", "03": "Osaka", "05": "Tokyo",
            "07": "Nagoya", "09": "Tokyo", "11": "Fukuoka",
        }[self.value]


class FatigueCurve(str, Enum):
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    S_CURVE = "s_curve"


@dataclass(frozen=True)
class WrestlerProfile:
    wrestler_id: str                          # Numeric API ID as string (e.g. "19")
    shikona: str                              # Current ring name in English (broadcast name)
    rank: Rank
    heya: str
    rank_number: Optional[int] = None
    side: Optional[str] = None
    height_cm: Optional[float] = None
    weight_kg: Optional[float] = None
    birth_date: Optional[date] = None
    fighting_style: FightingStyle = FightingStyle.HYBRID
    division: Division = Division.MAKUUCHI
    country: str = "Japan"
    # Extended fields for dossier
    shikona_jp: Optional[str] = None          # Kanji shikona (e.g. "豊昇龍")
    shikona_full: Optional[str] = None        # Full disambiguated name (e.g. "Kotozakura Masakatsu II")
    prefecture: Optional[str] = None          # Birth prefecture (Japanese wrestlers)
    api_id: Optional[int] = None              # Sumo API numeric ID
    highest_rank: Optional[str] = None        # Highest rank achieved
    highest_rank_number: Optional[int] = None
    is_active: bool = True
    debut_basho: Optional[str] = None
    career_wins: int = 0
    career_losses: int = 0
    career_absences: int = 0
    total_yusho: int = 0

    def __post_init__(self):
        if self.rank_number is not None and not (1 <= self.rank_number <= 18):
            raise ValueError(f"rank_number must be 1-18, got {self.rank_number}")
        if self.side is not None and self.side not in ("east", "west"):
            raise ValueError(f"side must be east or west, got {self.side}")

    @property
    def full_rank(self) -> str:
        parts = [self.rank.value.capitalize()]
        if self.rank_number is not None:
            parts.append(str(self.rank_number))
        if self.side:
            parts.append(self.side.capitalize())
        return " ".join(parts)

    @property
    def display_name(self) -> str:
        """Short name for display in tables and lists (broadcast name)."""
        return self.shikona

    @property
    def bmi(self) -> Optional[float]:
        if self.height_cm and self.weight_kg:
            h_m = self.height_cm / 100
            return round(self.weight_kg / (h_m * h_m), 1)
        return None


@dataclass(frozen=True)
class BoutRecord:
    basho_id: str
    day: int
    east_id: str
    west_id: str
    winner_id: str
    kimarite: Optional[str] = None

    def __post_init__(self):
        if not _BASHO_RE.match(self.basho_id):
            raise ValueError(f"basho_id must be YYYY.MM, got {self.basho_id}")
        if not (1 <= self.day <= 16):  # day 16 = kettei-sen (playoff)
            raise ValueError(f"day must be 1-16, got {self.day}")
        if self.winner_id not in (self.east_id, self.west_id):
            raise ValueError(
                f"winner_id {self.winner_id} must be east {self.east_id} or west {self.west_id}"
            )

    @property
    def loser_id(self) -> str:
        return self.west_id if self.winner_id == self.east_id else self.east_id


@dataclass(frozen=True)
class TournamentRecord:
    basho_id: str
    wrestler_id: str
    rank: Rank
    wins: int
    losses: int
    rank_number: Optional[int] = None
    side: Optional[str] = None
    absences: int = 0
    special_prizes: tuple = ()
    is_yusho: bool = False
    is_jun_yusho: bool = False

    def __post_init__(self):
        if not _BASHO_RE.match(self.basho_id):
            raise ValueError(f"basho_id must be YYYY.MM, got {self.basho_id}")
        if self.wins + self.losses + self.absences > 15:
            raise ValueError(f"wins+losses+absences exceeds 15")

    @property
    def total_bouts(self) -> int:
        return self.wins + self.losses

    @property
    def is_kachi_koshi(self) -> bool:
        return self.wins >= 8

    @property
    def is_make_koshi(self) -> bool:
        return self.losses >= 8

    @property
    def win_rate(self) -> Optional[float]:
        return self.wins / self.total_bouts if self.total_bouts else None


@dataclass(frozen=True)
class MatchupEntry:
    east_id: str
    west_id: str


@dataclass(frozen=True)
class BashoSchedule:
    basho_id: str
    day: int
    matchups: tuple = ()

    def __post_init__(self):
        if not _BASHO_RE.match(self.basho_id):
            raise ValueError(f"basho_id must be YYYY.MM, got {self.basho_id}")


@dataclass(frozen=True)
class HeadToHead:
    wrestler_a_id: str
    wrestler_b_id: str
    a_wins: int = 0
    b_wins: int = 0
    bouts: tuple = ()

    @property
    def total(self) -> int:
        return self.a_wins + self.b_wins

    def win_rate_for(self, wrestler_id: str) -> Optional[float]:
        if self.total == 0:
            return None
        if wrestler_id == self.wrestler_a_id:
            return self.a_wins / self.total
        elif wrestler_id == self.wrestler_b_id:
            return self.b_wins / self.total
        return None


@dataclass
class WrestlerRating:
    wrestler_id: str
    base_rating: float = 1500.0
    momentum_adjustment: float = 0.0
    matchup_adjustment: float = 0.0
    injury_fatigue_adjustment: float = 0.0

    @property
    def effective_rating(self) -> float:
        return (
            self.base_rating
            + self.momentum_adjustment
            + self.matchup_adjustment
            + self.injury_fatigue_adjustment
        )


@dataclass
class BoutResult:
    day: int
    east_id: str
    west_id: str
    east_win_probability: float
    west_win_probability: float
    winner_id: str
    confidence_interval_95: tuple = (0.0, 1.0)
    is_playoff: bool = False
    predicted_kimarite: Optional[str] = None

    @property
    def loser_id(self) -> str:
        return self.west_id if self.winner_id == self.east_id else self.east_id


@dataclass
class WrestlerStanding:
    wrestler_id: str
    shikona: str
    rank: Rank
    rank_number: Optional[int] = None
    wins: int = 0
    losses: int = 0

    @property
    def record(self) -> str:
        return f"{self.wins}-{self.losses}"


@dataclass
class TournamentResult:
    basho_id: str
    day_results: dict = field(default_factory=dict)
    final_standings: list = field(default_factory=list)
    yusho_winner_id: Optional[str] = None
    playoff_results: list = field(default_factory=list)

    @property
    def total_days_simulated(self) -> int:
        return len(self.day_results)


@dataclass
class TournamentProbabilities:
    basho_id: str
    num_simulations: int = 0
    yusho_probabilities: dict = field(default_factory=dict)
    kachi_koshi_probabilities: dict = field(default_factory=dict)
    average_wins: dict = field(default_factory=dict)
