"""
SumoSim Haru 2026 Data

Complete Makuuchi roster for the March 2026 basho in Osaka,
with tournament histories from the last 3 basho (Aki 2025,
Kyushu 2025, Hatsu 2026) used to calculate base ratings.

Data sourced from:
  - Official Haru 2026 banzuke (released Feb 24, 2026)
  - Fantasy Basho rikishi preview (Feb 25, 2026)
  - Hatsu 2026 day-by-day results (Sumo Stomp)
  - Wikipedia: 2026 in sumo

Key storylines:
  - Aonishiki (O1e) vying for 3rd straight yusho & yokozuna promotion
  - Hoshoryu & Onosato both dealing with injuries (knee, shoulder)
  - Kirishima seeking 3rd straight 11-4 for ozeki re-promotion
  - Atamifuji debuts at komusubi after playoff jun-yusho
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


def haru_2026_roster() -> list[WrestlerProfile]:
    """Complete 42-wrestler Makuuchi roster for Haru 2026."""
    return [
        # === Yokozuna ===
        WrestlerProfile(wrestler_id="19", shikona="Hoshoryu", rank=Rank.YOKOZUNA, rank_number=1, heya="Tatsunami", side="east", height_cm=188, weight_kg=150, birth_date=date(1999, 5, 22), fighting_style=FightingStyle.YOTSU, country="Mongolia"),
        WrestlerProfile(wrestler_id="8850", shikona="Onosato", rank=Rank.YOKOZUNA, rank_number=1, heya="Nishonoseki", side="west", height_cm=192, weight_kg=188, birth_date=date(2000, 6, 7), fighting_style=FightingStyle.YOTSU),

        # === Ozeki ===
        WrestlerProfile(wrestler_id="8854", shikona="Aonishiki", rank=Rank.OZEKI, rank_number=1, heya="Ajigawa", side="east", height_cm=182, weight_kg=140, birth_date=date(2004, 3, 23), fighting_style=FightingStyle.HYBRID, country="Ukraine"),
        WrestlerProfile(wrestler_id="20", shikona="Kotozakura", rank=Rank.OZEKI, rank_number=1, heya="Sadogatake", side="west", height_cm=189, weight_kg=178, birth_date=date(1997, 11, 19), fighting_style=FightingStyle.YOTSU),

        # === Sekiwake ===
        WrestlerProfile(wrestler_id="7", shikona="Kirishima", rank=Rank.SEKIWAKE, rank_number=1, heya="Otowayama", side="east", height_cm=186, weight_kg=149, birth_date=date(1996, 4, 24), fighting_style=FightingStyle.YOTSU, country="Mongolia"),
        WrestlerProfile(wrestler_id="44", shikona="Takayasu", rank=Rank.SEKIWAKE, rank_number=1, heya="Tagonoura", side="west", height_cm=188, weight_kg=172, birth_date=date(1990, 2, 28), fighting_style=FightingStyle.OSHI),

        # === Komusubi ===
        WrestlerProfile(wrestler_id="13", shikona="Wakamotoharu", rank=Rank.KOMUSUBI, rank_number=1, heya="Arashio", side="east", height_cm=187, weight_kg=150, birth_date=date(1993, 10, 5), fighting_style=FightingStyle.HYBRID),
        WrestlerProfile(wrestler_id="74", shikona="Atamifuji", rank=Rank.KOMUSUBI, rank_number=1, heya="Isegahama", side="west", height_cm=187, weight_kg=195, birth_date=date(2002, 9, 3), fighting_style=FightingStyle.YOTSU),

        # === Maegashira ===
        WrestlerProfile(wrestler_id="12", shikona="Wakatakakage", rank=Rank.MAEGASHIRA, rank_number=1, heya="Arashio", side="east", height_cm=183, weight_kg=138, birth_date=date(1994, 12, 6), fighting_style=FightingStyle.HYBRID),
        WrestlerProfile(wrestler_id="8857", shikona="Yoshinofuji", rank=Rank.MAEGASHIRA, rank_number=1, heya="Isegahama", side="west", height_cm=183, weight_kg=159, birth_date=date(2001, 6, 25), fighting_style=FightingStyle.OSHI),
        WrestlerProfile(wrestler_id="615", shikona="Fujinokawa", rank=Rank.MAEGASHIRA, rank_number=2, heya="Isenoumi", side="east", height_cm=177, weight_kg=122, birth_date=date(2005, 2, 22), fighting_style=FightingStyle.HYBRID),
        WrestlerProfile(wrestler_id="71", shikona="Churanoumi", rank=Rank.MAEGASHIRA, rank_number=2, heya="Kise", side="west", height_cm=178, weight_kg=153, birth_date=date(1993, 5, 6), fighting_style=FightingStyle.HYBRID),
        WrestlerProfile(wrestler_id="24", shikona="Hiradoumi", rank=Rank.MAEGASHIRA, rank_number=3, heya="Sakaigawa", side="east", height_cm=178, weight_kg=140, birth_date=date(2000, 4, 20), fighting_style=FightingStyle.YOTSU),
        WrestlerProfile(wrestler_id="41", shikona="Oho", rank=Rank.MAEGASHIRA, rank_number=3, heya="Otake", side="west", height_cm=191, weight_kg=180, birth_date=date(2000, 2, 14), fighting_style=FightingStyle.HYBRID),
        WrestlerProfile(wrestler_id="9", shikona="Daieisho", rank=Rank.MAEGASHIRA, rank_number=4, heya="Oitekaze", side="east", height_cm=183, weight_kg=166, birth_date=date(1993, 11, 10), fighting_style=FightingStyle.OSHI),
        WrestlerProfile(wrestler_id="37", shikona="Takanosho", rank=Rank.MAEGASHIRA, rank_number=4, heya="Minatogawa", side="west", height_cm=184, weight_kg=171, birth_date=date(1994, 11, 14), fighting_style=FightingStyle.OSHI),
        WrestlerProfile(wrestler_id="22", shikona="Abi", rank=Rank.MAEGASHIRA, rank_number=5, heya="Shikoroyama", side="east", height_cm=188, weight_kg=165, birth_date=date(1994, 5, 4), fighting_style=FightingStyle.OSHI),
        WrestlerProfile(wrestler_id="8", shikona="Kotoshoho", rank=Rank.MAEGASHIRA, rank_number=5, heya="Sadogatake", side="west", height_cm=191, weight_kg=173, birth_date=date(1999, 8, 26), fighting_style=FightingStyle.HYBRID),
        WrestlerProfile(wrestler_id="11", shikona="Ichiyamamoto", rank=Rank.MAEGASHIRA, rank_number=6, heya="Hanaregoma", side="east", height_cm=188, weight_kg=157, birth_date=date(1993, 10, 1), fighting_style=FightingStyle.OSHI),
        WrestlerProfile(wrestler_id="8853", shikona="Onokatsu", rank=Rank.MAEGASHIRA, rank_number=6, heya="Onomatsu", side="west", height_cm=185, weight_kg=174, birth_date=date(2000, 5, 5), fighting_style=FightingStyle.HYBRID),
        WrestlerProfile(wrestler_id="61", shikona="Oshoma", rank=Rank.MAEGASHIRA, rank_number=7, heya="Naruto", side="east", height_cm=190, weight_kg=164, birth_date=date(1997, 4, 9), fighting_style=FightingStyle.OSHI),
        WrestlerProfile(wrestler_id="3", shikona="Hakunofuji", rank=Rank.MAEGASHIRA, rank_number=7, heya="Isegahama", side="west", height_cm=181, weight_kg=155, birth_date=date(2003, 8, 22), fighting_style=FightingStyle.HYBRID),
        WrestlerProfile(wrestler_id="28", shikona="Ura", rank=Rank.MAEGASHIRA, rank_number=8, heya="Kise", side="east", height_cm=175, weight_kg=134, birth_date=date(1992, 6, 22), fighting_style=FightingStyle.HYBRID),
        WrestlerProfile(wrestler_id="33", shikona="Shodai", rank=Rank.MAEGASHIRA, rank_number=8, heya="Tokitsukaze", side="west", height_cm=184, weight_kg=169, birth_date=date(1991, 11, 5), fighting_style=FightingStyle.YOTSU),
        WrestlerProfile(wrestler_id="83", shikona="Tokihayate", rank=Rank.MAEGASHIRA, rank_number=9, heya="Tokitsukaze", side="east", height_cm=179, weight_kg=132, birth_date=date(1996, 8, 25), fighting_style=FightingStyle.HYBRID),
        WrestlerProfile(wrestler_id="14", shikona="Tamawashi", rank=Rank.MAEGASHIRA, rank_number=9, heya="Kataonami", side="west", height_cm=189, weight_kg=179, birth_date=date(1984, 11, 16), fighting_style=FightingStyle.OSHI, country="Mongolia"),
        WrestlerProfile(wrestler_id="56", shikona="Gonoyama", rank=Rank.MAEGASHIRA, rank_number=10, heya="Takekuma", side="east", height_cm=178, weight_kg=164, birth_date=date(1998, 4, 7), fighting_style=FightingStyle.OSHI),
        WrestlerProfile(wrestler_id="55", shikona="Roga", rank=Rank.MAEGASHIRA, rank_number=10, heya="Futagoyama", side="west", height_cm=184, weight_kg=159, birth_date=date(1999, 3, 2), fighting_style=FightingStyle.YOTSU),
        WrestlerProfile(wrestler_id="86", shikona="Shishi", rank=Rank.MAEGASHIRA, rank_number=11, heya="Ikazuchi", side="east", height_cm=193, weight_kg=169, birth_date=date(1997, 1, 16), fighting_style=FightingStyle.OSHI, country="Ukraine"),
        WrestlerProfile(wrestler_id="95", shikona="Oshoumi", rank=Rank.MAEGASHIRA, rank_number=11, heya="Naruto", side="west", height_cm=184, weight_kg=148, birth_date=date(2001, 5, 12), fighting_style=FightingStyle.YOTSU),
        WrestlerProfile(wrestler_id="164", shikona="Asakoryu", rank=Rank.MAEGASHIRA, rank_number=12, heya="Takasago", side="east", height_cm=178, weight_kg=123, birth_date=date(1998, 9, 24), fighting_style=FightingStyle.HYBRID),
        WrestlerProfile(wrestler_id="2", shikona="Asanoyama", rank=Rank.MAEGASHIRA, rank_number=12, heya="Takasago", side="west", height_cm=189, weight_kg=158, birth_date=date(1994, 3, 1), fighting_style=FightingStyle.YOTSU),
        WrestlerProfile(wrestler_id="21", shikona="Tobizaru", rank=Rank.MAEGASHIRA, rank_number=13, heya="Oitekaze", side="east", height_cm=174, weight_kg=130, birth_date=date(1992, 4, 24), fighting_style=FightingStyle.HYBRID),
        WrestlerProfile(wrestler_id="ryuden", shikona="Ryuden", rank=Rank.MAEGASHIRA, rank_number=13, heya="Takadagawa", side="west", height_cm=189, weight_kg=159, birth_date=date(1990, 11, 10), fighting_style=FightingStyle.YOTSU),
        WrestlerProfile(wrestler_id="50", shikona="Kinbozan", rank=Rank.MAEGASHIRA, rank_number=14, heya="Kise", side="east", height_cm=192, weight_kg=181, birth_date=date(1997, 6, 24), fighting_style=FightingStyle.OSHI, country="Kazakhstan"),
        WrestlerProfile(wrestler_id="26", shikona="Mitakeumi", rank=Rank.MAEGASHIRA, rank_number=14, heya="Dewanoumi", side="west", height_cm=181, weight_kg=175, birth_date=date(1992, 12, 25), fighting_style=FightingStyle.OSHI),
        WrestlerProfile(wrestler_id="607", shikona="Asahakuryu", rank=Rank.MAEGASHIRA, rank_number=15, heya="Takasago", side="east", height_cm=185, weight_kg=152, birth_date=date(2000, 1, 1), fighting_style=FightingStyle.HYBRID, country="Mongolia"),
        WrestlerProfile(wrestler_id="39", shikona="Chiyoshoma", rank=Rank.MAEGASHIRA, rank_number=15, heya="Kokonoe", side="west", height_cm=184, weight_kg=140, birth_date=date(1991, 7, 20), fighting_style=FightingStyle.HYBRID, country="Mongolia"),
        WrestlerProfile(wrestler_id="40", shikona="Nishikifuji", rank=Rank.MAEGASHIRA, rank_number=16, heya="Isegahama", side="east", height_cm=184, weight_kg=160, birth_date=date(1996, 7, 22), fighting_style=FightingStyle.YOTSU),
        WrestlerProfile(wrestler_id="82", shikona="Fujiseiun", rank=Rank.MAEGASHIRA, rank_number=16, heya="Fujishima", side="west", height_cm=180, weight_kg=155, birth_date=date(1997, 7, 15), fighting_style=FightingStyle.HYBRID),
        WrestlerProfile(wrestler_id="9051", shikona="Fujiryoga", rank=Rank.MAEGASHIRA, rank_number=17, heya="Fujishima", side="east", height_cm=183, weight_kg=160, birth_date=date(2003, 3, 5), fighting_style=FightingStyle.OSHI),
        WrestlerProfile(wrestler_id="112", shikona="Kotoeiho", rank=Rank.MAEGASHIRA, rank_number=17, heya="Sadogatake", side="west", height_cm=186, weight_kg=155, birth_date=date(1997, 5, 1), fighting_style=FightingStyle.HYBRID),
    ]


def haru_2026_tournament_records() -> dict[str, list[TournamentRecord]]:
    """
    Recent tournament records for base rating calculation.
    Covers Aki 2025 (2025.09), Kyushu 2025 (2025.11), Hatsu 2026 (2026.01).
    Weighted 20% / 30% / 50% respectively.
    """
    records = {}

    # --- Hoshoryu (Y1e) ---
    records["19"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="19", rank=Rank.YOKOZUNA, wins=11, losses=4),
        TournamentRecord(basho_id="2025.11", wrestler_id="19", rank=Rank.YOKOZUNA, wins=10, losses=5),
        TournamentRecord(basho_id="2026.01", wrestler_id="19", rank=Rank.YOKOZUNA, wins=10, losses=5),
    ]

    # --- Onosato (Y1w) ---
    records["8850"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="8850", rank=Rank.YOKOZUNA, wins=12, losses=3, is_yusho=True),
        TournamentRecord(basho_id="2025.11", wrestler_id="8850", rank=Rank.YOKOZUNA, wins=10, losses=5),
        TournamentRecord(basho_id="2026.01", wrestler_id="8850", rank=Rank.YOKOZUNA, wins=10, losses=5),
    ]

    # --- Aonishiki (O1e) — 2 consecutive yusho ---
    records["8854"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="8854", rank=Rank.SEKIWAKE, wins=11, losses=4),
        TournamentRecord(basho_id="2025.11", wrestler_id="8854", rank=Rank.SEKIWAKE, wins=14, losses=1, is_yusho=True),
        TournamentRecord(basho_id="2026.01", wrestler_id="8854", rank=Rank.OZEKI, wins=12, losses=3, is_yusho=True),
    ]

    # --- Kotozakura (O1w) ---
    records["20"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="20", rank=Rank.OZEKI, wins=9, losses=6),
        TournamentRecord(basho_id="2025.11", wrestler_id="20", rank=Rank.OZEKI, wins=8, losses=7),
        TournamentRecord(basho_id="2026.01", wrestler_id="20", rank=Rank.OZEKI, wins=8, losses=7),
    ]

    # --- Kirishima (S1e) — ozeki run ---
    records["7"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="7", rank=Rank.SEKIWAKE, wins=10, losses=5),
        TournamentRecord(basho_id="2025.11", wrestler_id="7", rank=Rank.SEKIWAKE, wins=11, losses=4),
        TournamentRecord(basho_id="2026.01", wrestler_id="7", rank=Rank.SEKIWAKE, wins=11, losses=4),
    ]

    # --- Takayasu (S1w) ---
    records["44"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="44", rank=Rank.KOMUSUBI, wins=9, losses=6),
        TournamentRecord(basho_id="2025.11", wrestler_id="44", rank=Rank.SEKIWAKE, wins=9, losses=6),
        TournamentRecord(basho_id="2026.01", wrestler_id="44", rank=Rank.SEKIWAKE, wins=8, losses=7),
    ]

    # --- Wakamotoharu (K1e) ---
    records["13"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="13", rank=Rank.MAEGASHIRA, rank_number=2, wins=9, losses=6),
        TournamentRecord(basho_id="2025.11", wrestler_id="13", rank=Rank.KOMUSUBI, wins=8, losses=7),
        TournamentRecord(basho_id="2026.01", wrestler_id="13", rank=Rank.KOMUSUBI, wins=8, losses=7),
    ]

    # --- Atamifuji (K1w) — jun-yusho Jan ---
    records["74"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="74", rank=Rank.MAEGASHIRA, rank_number=6, wins=8, losses=7),
        TournamentRecord(basho_id="2025.11", wrestler_id="74", rank=Rank.MAEGASHIRA, rank_number=5, wins=9, losses=6),
        TournamentRecord(basho_id="2026.01", wrestler_id="74", rank=Rank.MAEGASHIRA, rank_number=4, wins=12, losses=3, is_jun_yusho=True),
    ]

    # --- Wakatakakage (M1e) ---
    records["12"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="12", rank=Rank.MAEGASHIRA, rank_number=4, wins=8, losses=7),
        TournamentRecord(basho_id="2025.11", wrestler_id="12", rank=Rank.MAEGASHIRA, rank_number=4, wins=9, losses=6),
        TournamentRecord(basho_id="2026.01", wrestler_id="12", rank=Rank.MAEGASHIRA, rank_number=2, wins=9, losses=6),
    ]

    # --- Yoshinofuji (M1w) ---
    records["8857"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="8857", rank=Rank.MAEGASHIRA, rank_number=5, wins=9, losses=6),
        TournamentRecord(basho_id="2025.11", wrestler_id="8857", rank=Rank.MAEGASHIRA, rank_number=2, wins=8, losses=7),
        TournamentRecord(basho_id="2026.01", wrestler_id="8857", rank=Rank.MAEGASHIRA, rank_number=1, wins=8, losses=7),
    ]

    # --- Fujinokawa (M2e) — youngest wrestler ---
    records["615"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="615", rank=Rank.MAEGASHIRA, rank_number=9, wins=10, losses=5),
        TournamentRecord(basho_id="2025.11", wrestler_id="615", rank=Rank.MAEGASHIRA, rank_number=4, wins=8, losses=7),
        TournamentRecord(basho_id="2026.01", wrestler_id="615", rank=Rank.MAEGASHIRA, rank_number=7, wins=10, losses=5),
    ]

    # --- Churanoumi (M2w) ---
    records["71"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="71", rank=Rank.MAEGASHIRA, rank_number=7, wins=8, losses=7),
        TournamentRecord(basho_id="2025.11", wrestler_id="71", rank=Rank.MAEGASHIRA, rank_number=7, wins=8, losses=7),
        TournamentRecord(basho_id="2026.01", wrestler_id="71", rank=Rank.MAEGASHIRA, rank_number=5, wins=9, losses=6),
    ]

    # --- Hiradoumi (M3e) ---
    records["24"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="24", rank=Rank.MAEGASHIRA, rank_number=8, wins=9, losses=6),
        TournamentRecord(basho_id="2025.11", wrestler_id="24", rank=Rank.MAEGASHIRA, rank_number=5, wins=8, losses=7),
        TournamentRecord(basho_id="2026.01", wrestler_id="24", rank=Rank.MAEGASHIRA, rank_number=6, wins=9, losses=6),
    ]

    # --- Oho (M3w) ---
    records["41"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="41", rank=Rank.MAEGASHIRA, rank_number=3, wins=9, losses=6),
        TournamentRecord(basho_id="2025.11", wrestler_id="41", rank=Rank.MAEGASHIRA, rank_number=1, wins=8, losses=7),
        TournamentRecord(basho_id="2026.01", wrestler_id="41", rank=Rank.KOMUSUBI, wins=4, losses=11),
    ]

    # --- Daieisho (M4e) ---
    records["9"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="9", rank=Rank.MAEGASHIRA, rank_number=5, wins=7, losses=8),
        TournamentRecord(basho_id="2025.11", wrestler_id="9", rank=Rank.MAEGASHIRA, rank_number=6, wins=9, losses=6),
        TournamentRecord(basho_id="2026.01", wrestler_id="9", rank=Rank.MAEGASHIRA, rank_number=4, wins=7, losses=8),
    ]

    # --- Takanosho (M4w) ---
    records["37"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="37", rank=Rank.MAEGASHIRA, rank_number=4, wins=7, losses=8),
        TournamentRecord(basho_id="2025.11", wrestler_id="37", rank=Rank.MAEGASHIRA, rank_number=5, wins=9, losses=6),
        TournamentRecord(basho_id="2026.01", wrestler_id="37", rank=Rank.MAEGASHIRA, rank_number=3, wins=5, losses=10),
    ]

    # --- Abi (M5e) ---
    records["22"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="22", rank=Rank.MAEGASHIRA, rank_number=10, wins=10, losses=5),
        TournamentRecord(basho_id="2025.11", wrestler_id="22", rank=Rank.MAEGASHIRA, rank_number=5, wins=6, losses=9),
        TournamentRecord(basho_id="2026.01", wrestler_id="22", rank=Rank.MAEGASHIRA, rank_number=12, wins=10, losses=5),
    ]

    # --- Kotoshoho (M5w) ---
    records["8"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="8", rank=Rank.MAEGASHIRA, rank_number=12, wins=10, losses=5),
        TournamentRecord(basho_id="2025.11", wrestler_id="8", rank=Rank.MAEGASHIRA, rank_number=7, wins=7, losses=8),
        TournamentRecord(basho_id="2026.01", wrestler_id="8", rank=Rank.MAEGASHIRA, rank_number=10, wins=9, losses=6),
    ]

    # --- Ichiyamamoto (M6e) ---
    records["11"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="11", rank=Rank.MAEGASHIRA, rank_number=3, wins=6, losses=9),
        TournamentRecord(basho_id="2025.11", wrestler_id="11", rank=Rank.MAEGASHIRA, rank_number=6, wins=10, losses=5),
        TournamentRecord(basho_id="2026.01", wrestler_id="11", rank=Rank.MAEGASHIRA, rank_number=1, wins=4, losses=11),
    ]

    # --- Onokatsu (M6w) ---
    records["8853"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="8853", rank=Rank.MAEGASHIRA, rank_number=6, wins=7, losses=8),
        TournamentRecord(basho_id="2025.11", wrestler_id="8853", rank=Rank.MAEGASHIRA, rank_number=6, wins=7, losses=8),
        TournamentRecord(basho_id="2026.01", wrestler_id="8853", rank=Rank.MAEGASHIRA, rank_number=6, wins=7, losses=8),
    ]

    # --- Oshoma (M7e) ---
    records["61"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="61", rank=Rank.MAEGASHIRA, rank_number=8, wins=9, losses=6),
        TournamentRecord(basho_id="2025.11", wrestler_id="61", rank=Rank.MAEGASHIRA, rank_number=3, wins=4, losses=11),
        TournamentRecord(basho_id="2026.01", wrestler_id="61", rank=Rank.MAEGASHIRA, rank_number=7, wins=7, losses=8),
    ]

    # --- Hakunofuji (M7w) ---
    records["3"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="3", rank=Rank.MAEGASHIRA, rank_number=6, wins=8, losses=7),
        TournamentRecord(basho_id="2025.11", wrestler_id="3", rank=Rank.MAEGASHIRA, rank_number=5, wins=9, losses=6),
        TournamentRecord(basho_id="2026.01", wrestler_id="3", rank=Rank.MAEGASHIRA, rank_number=3, wins=5, losses=8, absences=2),
    ]

    # --- Ura (M8e) ---
    records["28"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="28", rank=Rank.MAEGASHIRA, rank_number=5, wins=7, losses=8),
        TournamentRecord(basho_id="2025.11", wrestler_id="28", rank=Rank.MAEGASHIRA, rank_number=6, wins=9, losses=6),
        TournamentRecord(basho_id="2026.01", wrestler_id="28", rank=Rank.MAEGASHIRA, rank_number=2, wins=4, losses=11),
    ]

    # --- Shodai (M8w) ---
    records["33"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="33", rank=Rank.MAEGASHIRA, rank_number=9, wins=8, losses=7),
        TournamentRecord(basho_id="2025.11", wrestler_id="33", rank=Rank.MAEGASHIRA, rank_number=9, wins=8, losses=7),
        TournamentRecord(basho_id="2026.01", wrestler_id="33", rank=Rank.MAEGASHIRA, rank_number=8, wins=7, losses=8),
    ]

    # --- Tokihayate (M9e) ---
    records["83"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="83", rank=Rank.MAEGASHIRA, rank_number=12, wins=9, losses=6),
        TournamentRecord(basho_id="2025.11", wrestler_id="83", rank=Rank.MAEGASHIRA, rank_number=8, wins=8, losses=7),
        TournamentRecord(basho_id="2026.01", wrestler_id="83", rank=Rank.MAEGASHIRA, rank_number=10, wins=8, losses=7),
    ]

    # --- Tamawashi (M9w) — oldest wrestler at 41 ---
    records["14"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="14", rank=Rank.MAEGASHIRA, rank_number=7, wins=6, losses=9),
        TournamentRecord(basho_id="2025.11", wrestler_id="14", rank=Rank.MAEGASHIRA, rank_number=9, wins=9, losses=6),
        TournamentRecord(basho_id="2026.01", wrestler_id="14", rank=Rank.MAEGASHIRA, rank_number=5, wins=5, losses=10),
    ]

    # --- Gonoyama (M10e) ---
    records["56"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="56", rank=Rank.MAEGASHIRA, rank_number=11, wins=9, losses=6),
        TournamentRecord(basho_id="2025.11", wrestler_id="56", rank=Rank.MAEGASHIRA, rank_number=8, wins=7, losses=8),
        TournamentRecord(basho_id="2026.01", wrestler_id="56", rank=Rank.MAEGASHIRA, rank_number=9, wins=7, losses=8),
    ]

    # --- Roga (M10w) ---
    records["55"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="55", rank=Rank.MAEGASHIRA, rank_number=11, wins=8, losses=7),
        TournamentRecord(basho_id="2025.11", wrestler_id="55", rank=Rank.MAEGASHIRA, rank_number=10, wins=8, losses=7),
        TournamentRecord(basho_id="2026.01", wrestler_id="55", rank=Rank.MAEGASHIRA, rank_number=9, wins=7, losses=8),
    ]

    # --- Shishi (M11e) ---
    records["86"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="86", rank=Rank.MAEGASHIRA, rank_number=15, wins=9, losses=6),
        TournamentRecord(basho_id="2025.11", wrestler_id="86", rank=Rank.MAEGASHIRA, rank_number=12, wins=8, losses=7),
        TournamentRecord(basho_id="2026.01", wrestler_id="86", rank=Rank.MAEGASHIRA, rank_number=14, wins=9, losses=6),
    ]

    # --- Oshoumi (M11w) ---
    records["95"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="95", rank=Rank.MAEGASHIRA, rank_number=16, wins=8, losses=7),
        TournamentRecord(basho_id="2025.11", wrestler_id="95", rank=Rank.MAEGASHIRA, rank_number=15, wins=7, losses=8),
        TournamentRecord(basho_id="2026.01", wrestler_id="95", rank=Rank.MAEGASHIRA, rank_number=16, wins=10, losses=5),
    ]

    # --- Asakoryu (M12e) ---
    records["164"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="164", rank=Rank.MAEGASHIRA, rank_number=14, wins=8, losses=7),
        TournamentRecord(basho_id="2025.11", wrestler_id="164", rank=Rank.MAEGASHIRA, rank_number=14, wins=8, losses=7),
        TournamentRecord(basho_id="2026.01", wrestler_id="164", rank=Rank.MAEGASHIRA, rank_number=15, wins=9, losses=6),
    ]

    # --- Asanoyama (M12w) ---
    records["2"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="2", rank=Rank.MAEGASHIRA, rank_number=13, wins=9, losses=6),
        TournamentRecord(basho_id="2025.11", wrestler_id="2", rank=Rank.MAEGASHIRA, rank_number=10, wins=7, losses=8),
        TournamentRecord(basho_id="2026.01", wrestler_id="2", rank=Rank.MAEGASHIRA, rank_number=13, wins=9, losses=6),
    ]

    # --- Tobizaru (M13e) ---
    records["21"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="21", rank=Rank.MAEGASHIRA, rank_number=10, wins=6, losses=9),
        TournamentRecord(basho_id="2025.11", wrestler_id="21", rank=Rank.MAEGASHIRA, rank_number=11, wins=8, losses=7),
        TournamentRecord(basho_id="2026.01", wrestler_id="21", rank=Rank.MAEGASHIRA, rank_number=13, wins=7, losses=8),
    ]

    # --- Midorifuji (M13w) ---
    # --- Ryuden (M13w) — Juryo backfill for kyujo Midorifuji ---
    records["ryuden"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="ryuden", rank=Rank.MAEGASHIRA, wins=9, losses=6),
        TournamentRecord(basho_id="2025.11", wrestler_id="ryuden", rank=Rank.MAEGASHIRA, rank_number=14, wins=2, losses=1),
        TournamentRecord(basho_id="2026.01", wrestler_id="ryuden", rank=Rank.MAEGASHIRA, rank_number=15, wins=6, losses=9),
    ]

    # --- Kinbozan (M14e) ---
    records["50"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="50", rank=Rank.MAEGASHIRA, rank_number=13, wins=7, losses=8),
        TournamentRecord(basho_id="2025.11", wrestler_id="50", rank=Rank.MAEGASHIRA, rank_number=13, wins=8, losses=7),
        TournamentRecord(basho_id="2026.01", wrestler_id="50", rank=Rank.MAEGASHIRA, rank_number=8, wins=4, losses=11),
    ]

    # --- Mitakeumi (M14w) ---
    records["26"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="26", rank=Rank.MAEGASHIRA, rank_number=14, wins=8, losses=7),
        TournamentRecord(basho_id="2025.11", wrestler_id="26", rank=Rank.MAEGASHIRA, rank_number=13, wins=8, losses=7),
        TournamentRecord(basho_id="2026.01", wrestler_id="26", rank=Rank.MAEGASHIRA, rank_number=14, wins=7, losses=8),
    ]

    # --- Asahakuryu (M15e) ---
    records["607"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="607", rank=Rank.MAEGASHIRA, rank_number=15, wins=9, losses=6),
        TournamentRecord(basho_id="2025.11", wrestler_id="607", rank=Rank.MAEGASHIRA, rank_number=11, wins=6, losses=9),
        TournamentRecord(basho_id="2026.01", wrestler_id="607", rank=Rank.MAEGASHIRA, rank_number=16, wins=5, losses=10),
    ]

    # --- Chiyoshoma (M15w) ---
    records["39"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="39", rank=Rank.MAEGASHIRA, rank_number=13, wins=7, losses=8),
        TournamentRecord(basho_id="2025.11", wrestler_id="39", rank=Rank.MAEGASHIRA, rank_number=13, wins=8, losses=7),
        TournamentRecord(basho_id="2026.01", wrestler_id="39", rank=Rank.MAEGASHIRA, rank_number=11, wins=6, losses=9),
    ]

    # --- Nishikifuji (M16e) ---
    records["40"] = [
        TournamentRecord(basho_id="2025.09", wrestler_id="40", rank=Rank.MAEGASHIRA, rank_number=16, wins=8, losses=7),
        TournamentRecord(basho_id="2025.11", wrestler_id="40", rank=Rank.MAEGASHIRA, rank_number=15, wins=7, losses=8),
        TournamentRecord(basho_id="2026.01", wrestler_id="40", rank=Rank.MAEGASHIRA, rank_number=11, wins=6, losses=6, absences=3),
    ]

    # --- Fujiseiun (M16w) — debut ---
    records["82"] = [
        TournamentRecord(basho_id="2026.01", wrestler_id="82", rank=Rank.MAEGASHIRA, rank_number=16, wins=8, losses=7),
    ]

    # --- Fujiryoga (M17e) — debut ---
    records["9051"] = [
        TournamentRecord(basho_id="2026.01", wrestler_id="9051", rank=Rank.MAEGASHIRA, rank_number=17, wins=8, losses=7),
    ]

    # --- Kotoeiho (M17w) ---
    records["112"] = [
        TournamentRecord(basho_id="2025.11", wrestler_id="112", rank=Rank.MAEGASHIRA, rank_number=16, wins=7, losses=8),
        TournamentRecord(basho_id="2026.01", wrestler_id="112", rank=Rank.MAEGASHIRA, rank_number=17, wins=8, losses=7),
    ]

    return records


def haru_2026_injury_notes() -> dict[str, dict]:
    """
    Known injury/health concerns entering Haru 2026.
    These can be used to set initial injury severity in the modifier panel.
    Severity: 0.0 = healthy, 1.0 = severely compromised.
    """
    return {
        "19": {"severity": 0.35, "note": "Right knee injury, visible in January"},
        "8850": {"severity": 0.30, "note": "Left shoulder, wincing on contact in January"},
        "20": {"severity": 0.20, "note": "Chronic condition, not 100% for over a year"},
        "3": {"severity": 0.40, "note": "Left big toe ligament + possible knee, withdrew Day 13"},
        "40": {"severity": 0.25, "note": "Withdrew Day 12 of Hatsu, ongoing neck/back pain"},
        "9": {"severity": 0.15, "note": "Recovering from July absence, slow start in January"},
        "14": {"severity": 0.10, "note": "Age-related fatigue at 41, no specific injury"},
        # Midorifuji is kyujo (heart failure) — replaced by Ryuden in roster
        # Kept here for reference even though he's not in the active roster
    }
