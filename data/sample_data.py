"""
SumoSim Sample Data

Realistic mock data for offline development and testing.
Based on the actual Hatsu 2025 Makuuchi roster from sumodb.
"""

from __future__ import annotations

from datetime import date

from data.models import (
    BoutRecord,
    FightingStyle,
    Rank,
    TournamentRecord,
    WrestlerProfile,
)


def sample_roster() -> list[WrestlerProfile]:
    """A realistic 20-wrestler subset of the Makuuchi division."""
    return [
        WrestlerProfile(wrestler_id="sr_onosato", shikona="Onosato", rank=Rank.YOKOZUNA, heya="Nishonoseki", side="east", height_cm=192.0, weight_kg=177.0, birth_date=date(2000, 6, 7), fighting_style=FightingStyle.YOTSU),
        WrestlerProfile(wrestler_id="sr_kotozakura", shikona="Kotozakura", rank=Rank.OZEKI, rank_number=1, heya="Sadogatake", side="east", height_cm=189.0, weight_kg=175.0, birth_date=date(1997, 5, 13), fighting_style=FightingStyle.YOTSU),
        WrestlerProfile(wrestler_id="sr_hoshoryu", shikona="Hoshoryu", rank=Rank.OZEKI, rank_number=1, heya="Tatsunami", side="west", height_cm=188.0, weight_kg=150.0, birth_date=date(1999, 5, 22), fighting_style=FightingStyle.YOTSU),
        WrestlerProfile(wrestler_id="sr_aonishiki", shikona="Aonishiki", rank=Rank.OZEKI, rank_number=2, heya="Futagoyama", side="east", height_cm=184.0, weight_kg=162.0, birth_date=date(2000, 3, 3), fighting_style=FightingStyle.OSHI),
        WrestlerProfile(wrestler_id="sr_kirishima", shikona="Kirishima", rank=Rank.SEKIWAKE, rank_number=1, heya="Michinoku", side="east", height_cm=186.0, weight_kg=143.0, birth_date=date(1996, 4, 24), fighting_style=FightingStyle.YOTSU),
        WrestlerProfile(wrestler_id="sr_abi", shikona="Abi", rank=Rank.SEKIWAKE, rank_number=1, heya="Shikoroyama", side="west", height_cm=186.0, weight_kg=158.0, birth_date=date(1994, 5, 4), fighting_style=FightingStyle.OSHI),
        WrestlerProfile(wrestler_id="sr_daieisho", shikona="Daieisho", rank=Rank.KOMUSUBI, rank_number=1, heya="Oitekaze", side="east", height_cm=182.0, weight_kg=164.0, birth_date=date(1993, 11, 10), fighting_style=FightingStyle.OSHI),
        WrestlerProfile(wrestler_id="sr_wakamotoharu", shikona="Wakamotoharu", rank=Rank.KOMUSUBI, rank_number=1, heya="Arashio", side="west", height_cm=187.0, weight_kg=147.0, birth_date=date(1993, 10, 5), fighting_style=FightingStyle.HYBRID),
        WrestlerProfile(wrestler_id="sr_fujinokawa", shikona="Fujinokawa", rank=Rank.MAEGASHIRA, rank_number=1, heya="Fujishima", side="east", height_cm=185.0, weight_kg=160.0, birth_date=date(1999, 12, 1), fighting_style=FightingStyle.HYBRID),
        WrestlerProfile(wrestler_id="sr_oho", shikona="Oho", rank=Rank.MAEGASHIRA, rank_number=2, heya="Otake", side="east", height_cm=190.0, weight_kg=179.0, birth_date=date(2000, 2, 14), fighting_style=FightingStyle.HYBRID),
        WrestlerProfile(wrestler_id="sr_kinbozan", shikona="Kinbozan", rank=Rank.MAEGASHIRA, rank_number=3, heya="Kise", side="east", height_cm=192.0, weight_kg=181.0, birth_date=date(1997, 6, 24), fighting_style=FightingStyle.OSHI),
        WrestlerProfile(wrestler_id="sr_takayasu", shikona="Takayasu", rank=Rank.MAEGASHIRA, rank_number=4, heya="Tagonoura", side="east", height_cm=188.0, weight_kg=183.0, birth_date=date(1990, 2, 28), fighting_style=FightingStyle.OSHI),
        WrestlerProfile(wrestler_id="sr_ichiyamamoto", shikona="Ichiyamamoto", rank=Rank.MAEGASHIRA, rank_number=5, heya="Hanaregoma", side="east", height_cm=187.0, weight_kg=144.0, birth_date=date(1993, 10, 1), fighting_style=FightingStyle.OSHI),
        WrestlerProfile(wrestler_id="sr_ura", shikona="Ura", rank=Rank.MAEGASHIRA, rank_number=6, heya="Kise", side="east", height_cm=175.0, weight_kg=143.0, birth_date=date(1992, 6, 22), fighting_style=FightingStyle.HYBRID),
        WrestlerProfile(wrestler_id="sr_oshoma", shikona="Oshoma", rank=Rank.MAEGASHIRA, rank_number=7, heya="Naruto", side="east", height_cm=189.0, weight_kg=160.0, birth_date=date(1997, 4, 9), fighting_style=FightingStyle.HYBRID),
        WrestlerProfile(wrestler_id="sr_shodai", shikona="Shodai", rank=Rank.MAEGASHIRA, rank_number=8, heya="Tokitsukaze", side="west", height_cm=184.0, weight_kg=161.0, birth_date=date(1991, 11, 5), fighting_style=FightingStyle.YOTSU),
        WrestlerProfile(wrestler_id="sr_tamawashi", shikona="Tamawashi", rank=Rank.MAEGASHIRA, rank_number=9, heya="Kataonami", side="east", height_cm=189.0, weight_kg=174.0, birth_date=date(1984, 11, 16), fighting_style=FightingStyle.OSHI),
        WrestlerProfile(wrestler_id="sr_asanoyama", shikona="Asanoyama", rank=Rank.MAEGASHIRA, rank_number=10, heya="Takasago", side="east", height_cm=189.0, weight_kg=168.0, birth_date=date(1994, 3, 1), fighting_style=FightingStyle.YOTSU),
        WrestlerProfile(wrestler_id="sr_tobizaru", shikona="Tobizaru", rank=Rank.MAEGASHIRA, rank_number=12, heya="Oitekaze", side="west", height_cm=174.0, weight_kg=135.0, birth_date=date(1992, 4, 24), fighting_style=FightingStyle.HYBRID),
        WrestlerProfile(wrestler_id="sr_midorifuji", shikona="Midorifuji", rank=Rank.MAEGASHIRA, rank_number=15, heya="Isegahama", side="east", height_cm=171.0, weight_kg=116.0, birth_date=date(1996, 8, 30), fighting_style=FightingStyle.HYBRID),
    ]


def sample_bout_records() -> list[BoutRecord]:
    """Sample bout results for Hatsu 2025, Days 1-3."""
    return [
        # Day 1
        BoutRecord(basho_id="2025.01", day=1, east_id="sr_onosato", west_id="sr_daieisho", winner_id="sr_onosato", kimarite="yorikiri"),
        BoutRecord(basho_id="2025.01", day=1, east_id="sr_hoshoryu", west_id="sr_fujinokawa", winner_id="sr_hoshoryu", kimarite="uwatenage"),
        BoutRecord(basho_id="2025.01", day=1, east_id="sr_kotozakura", west_id="sr_abi", winner_id="sr_abi", kimarite="oshidashi"),
        BoutRecord(basho_id="2025.01", day=1, east_id="sr_aonishiki", west_id="sr_oho", winner_id="sr_aonishiki", kimarite="yorikiri"),
        BoutRecord(basho_id="2025.01", day=1, east_id="sr_kirishima", west_id="sr_kinbozan", winner_id="sr_kirishima", kimarite="uwatenage"),
        BoutRecord(basho_id="2025.01", day=1, east_id="sr_takayasu", west_id="sr_wakamotoharu", winner_id="sr_takayasu", kimarite="oshidashi"),
        BoutRecord(basho_id="2025.01", day=1, east_id="sr_ichiyamamoto", west_id="sr_shodai", winner_id="sr_shodai", kimarite="yorikiri"),
        BoutRecord(basho_id="2025.01", day=1, east_id="sr_ura", west_id="sr_tamawashi", winner_id="sr_tamawashi", kimarite="tsukidashi"),
        BoutRecord(basho_id="2025.01", day=1, east_id="sr_oshoma", west_id="sr_tobizaru", winner_id="sr_oshoma", kimarite="hatakikomi"),
        BoutRecord(basho_id="2025.01", day=1, east_id="sr_asanoyama", west_id="sr_midorifuji", winner_id="sr_asanoyama", kimarite="yorikiri"),
        # Day 2
        BoutRecord(basho_id="2025.01", day=2, east_id="sr_onosato", west_id="sr_wakamotoharu", winner_id="sr_onosato", kimarite="oshidashi"),
        BoutRecord(basho_id="2025.01", day=2, east_id="sr_hoshoryu", west_id="sr_kinbozan", winner_id="sr_hoshoryu", kimarite="yorikiri"),
        BoutRecord(basho_id="2025.01", day=2, east_id="sr_kotozakura", west_id="sr_daieisho", winner_id="sr_kotozakura", kimarite="yorikiri"),
        BoutRecord(basho_id="2025.01", day=2, east_id="sr_aonishiki", west_id="sr_kirishima", winner_id="sr_kirishima", kimarite="uwatenage"),
        BoutRecord(basho_id="2025.01", day=2, east_id="sr_abi", west_id="sr_oho", winner_id="sr_abi", kimarite="oshidashi"),
        BoutRecord(basho_id="2025.01", day=2, east_id="sr_takayasu", west_id="sr_fujinokawa", winner_id="sr_fujinokawa", kimarite="hatakikomi"),
        BoutRecord(basho_id="2025.01", day=2, east_id="sr_ura", west_id="sr_oshoma", winner_id="sr_ura", kimarite="shitatenage"),
        BoutRecord(basho_id="2025.01", day=2, east_id="sr_shodai", west_id="sr_tamawashi", winner_id="sr_tamawashi", kimarite="oshidashi"),
        BoutRecord(basho_id="2025.01", day=2, east_id="sr_asanoyama", west_id="sr_tobizaru", winner_id="sr_asanoyama", kimarite="yorikiri"),
        BoutRecord(basho_id="2025.01", day=2, east_id="sr_ichiyamamoto", west_id="sr_midorifuji", winner_id="sr_midorifuji", kimarite="hatakikomi"),
        # Day 3
        BoutRecord(basho_id="2025.01", day=3, east_id="sr_onosato", west_id="sr_hoshoryu", winner_id="sr_onosato", kimarite="yorikiri"),
        BoutRecord(basho_id="2025.01", day=3, east_id="sr_kotozakura", west_id="sr_kirishima", winner_id="sr_kotozakura", kimarite="oshidashi"),
        BoutRecord(basho_id="2025.01", day=3, east_id="sr_aonishiki", west_id="sr_abi", winner_id="sr_aonishiki", kimarite="tsukiotoshi"),
        BoutRecord(basho_id="2025.01", day=3, east_id="sr_daieisho", west_id="sr_oho", winner_id="sr_daieisho", kimarite="oshidashi"),
        BoutRecord(basho_id="2025.01", day=3, east_id="sr_wakamotoharu", west_id="sr_kinbozan", winner_id="sr_kinbozan", kimarite="yorikiri"),
        BoutRecord(basho_id="2025.01", day=3, east_id="sr_takayasu", west_id="sr_ura", winner_id="sr_takayasu", kimarite="oshidashi"),
        BoutRecord(basho_id="2025.01", day=3, east_id="sr_fujinokawa", west_id="sr_tamawashi", winner_id="sr_fujinokawa", kimarite="yorikiri"),
        BoutRecord(basho_id="2025.01", day=3, east_id="sr_shodai", west_id="sr_asanoyama", winner_id="sr_asanoyama", kimarite="uwatenage"),
        BoutRecord(basho_id="2025.01", day=3, east_id="sr_tobizaru", west_id="sr_midorifuji", winner_id="sr_tobizaru", kimarite="hatakikomi"),
        BoutRecord(basho_id="2025.01", day=3, east_id="sr_oshoma", west_id="sr_ichiyamamoto", winner_id="sr_oshoma", kimarite="oshidashi"),
    ]


def sample_tournament_records() -> dict[str, list[TournamentRecord]]:
    """Recent tournament records keyed by wrestler_id (3 basho each)."""
    data = {
        "sr_onosato": [
            TournamentRecord(basho_id="2025.01", wrestler_id="sr_onosato", rank=Rank.YOKOZUNA, wins=14, losses=1, is_yusho=True),
            TournamentRecord(basho_id="2024.11", wrestler_id="sr_onosato", rank=Rank.OZEKI, wins=13, losses=2, is_yusho=True),
            TournamentRecord(basho_id="2024.09", wrestler_id="sr_onosato", rank=Rank.OZEKI, wins=13, losses=2),
        ],
        "sr_kotozakura": [
            TournamentRecord(basho_id="2025.01", wrestler_id="sr_kotozakura", rank=Rank.OZEKI, wins=10, losses=5),
            TournamentRecord(basho_id="2024.11", wrestler_id="sr_kotozakura", rank=Rank.OZEKI, wins=11, losses=4),
            TournamentRecord(basho_id="2024.09", wrestler_id="sr_kotozakura", rank=Rank.OZEKI, wins=12, losses=3),
        ],
        "sr_hoshoryu": [
            TournamentRecord(basho_id="2025.01", wrestler_id="sr_hoshoryu", rank=Rank.OZEKI, wins=11, losses=4),
            TournamentRecord(basho_id="2024.11", wrestler_id="sr_hoshoryu", rank=Rank.OZEKI, wins=10, losses=5),
            TournamentRecord(basho_id="2024.09", wrestler_id="sr_hoshoryu", rank=Rank.OZEKI, wins=9, losses=6),
        ],
        "sr_abi": [
            TournamentRecord(basho_id="2025.01", wrestler_id="sr_abi", rank=Rank.SEKIWAKE, wins=9, losses=6),
            TournamentRecord(basho_id="2024.11", wrestler_id="sr_abi", rank=Rank.SEKIWAKE, wins=10, losses=5),
            TournamentRecord(basho_id="2024.09", wrestler_id="sr_abi", rank=Rank.SEKIWAKE, wins=8, losses=7),
        ],
        "sr_kirishima": [
            TournamentRecord(basho_id="2025.01", wrestler_id="sr_kirishima", rank=Rank.SEKIWAKE, wins=8, losses=7),
            TournamentRecord(basho_id="2024.11", wrestler_id="sr_kirishima", rank=Rank.SEKIWAKE, wins=7, losses=8),
            TournamentRecord(basho_id="2024.09", wrestler_id="sr_kirishima", rank=Rank.OZEKI, wins=6, losses=9),
        ],
    }
    return data
