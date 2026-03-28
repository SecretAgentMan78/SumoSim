"""
SumoSim Sumo Reference Scraper

Scrapes data from sumodb.sumogames.de using their text-format pages,
which are simpler and more stable than the HTML table pages.

Pages used:
    Banzuke_text.aspx?b=YYYYMM    — Roster with ranks, heya, measurements
    Results_text.aspx?b=YYYYMM&d=N — Bout results for a specific day

Text format (banzuke):
    Y1e    Terunofuji     Mongolia  Isegahama   29.11.1991    192   174
    O1e    Takakeisho     Hyogo     Tokiwayama  05.08.1996    175   165
    M1e    Nishikigi      Iwate     Isenoumi    25.08.1990    185   180

Text format (results):
    Onosato oshidashi Hoshoryu
    Abi uwatenage Kotozakura
"""

from __future__ import annotations

import logging
import re
import time
from datetime import date as Date
from typing import Optional

import requests
from bs4 import BeautifulSoup

from data.models import (
    BoutRecord,
    FightingStyle,
    Rank,
    TournamentRecord,
    WrestlerProfile,
)
from utils.config import get_config

logger = logging.getLogger(__name__)

BASE_URL = "http://sumodb.sumogames.de"

# Regex for parsing banzuke text lines
# Example: "Y1e    Terunofuji     Mongolia  Isegahama   29.11.1991    192   174"
# Rank part: Y=yokozuna, O=ozeki, S=sekiwake, K=komusubi, M=maegashira
# Number + side: 1e, 1w, 2e, etc.
_RANK_PREFIX_MAP = {
    "Y": Rank.YOKOZUNA,
    "O": Rank.OZEKI,
    "S": Rank.SEKIWAKE,
    "K": Rank.KOMUSUBI,
    "M": Rank.MAEGASHIRA,
}

_BANZUKE_LINE_RE = re.compile(
    r"^([YOSKM])(\d+)(e|w)\s+"     # Rank code + number + east/west
    r"(\S+)\s+"                     # Shikona
    r"(.+?)\s+"                    # Birthplace (+ possibly heya if merged)
    r"(\d{2}\.\d{2}\.\d{4})\s+"   # Birth date (DD.MM.YYYY)
    r"(\d+(?:\.\d+)?)\s+"         # Height
    r"(\d+(?:\.\d+)?)"            # Weight
)

# Divisions we care about for parsing
_MAKUUCHI_DIVISIONS = {"Makuuchi"}


class SumoScraper:
    """
    Scrapes data from Sumo Reference (sumodb.sumogames.de).

    Uses the text-format pages for reliability — they have a simpler,
    more stable format than the HTML table pages.

    Usage:
        scraper = SumoScraper()
        roster = scraper.fetch_banzuke("2025.01")
        results = scraper.fetch_day_results("2025.01", day=1)
    """

    def __init__(self, delay_ms: int | None = None):
        cfg = get_config()
        self._delay_s = (delay_ms or cfg.scrape_delay_ms) / 1000.0
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "SumoSim/1.0 (sumo tournament simulator; educational use)",
        })
        self._last_request_time = 0.0

    def close(self) -> None:
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    # ------------------------------------------------------------------
    # Public scraping methods
    # ------------------------------------------------------------------

    def fetch_banzuke(self, basho_id: str) -> list[WrestlerProfile]:
        """
        Scrape the Makuuchi banzuke for a given tournament.

        Args:
            basho_id: Tournament ID in 'YYYY.MM' format (e.g., '2025.01').

        Returns:
            List of WrestlerProfile objects for all Makuuchi wrestlers.
        """
        api_basho = basho_id.replace(".", "")
        url = f"{BASE_URL}/Banzuke_text.aspx?b={api_basho}&l=e"

        html = self._fetch_page(url)
        if not html:
            return []

        return self._parse_banzuke_text(html)

    def fetch_day_results(
        self, basho_id: str, day: int
    ) -> list[BoutRecord]:
        """
        Scrape bout results for a specific day.

        Args:
            basho_id: Tournament ID in 'YYYY.MM' format.
            day: Day number (1-15).

        Returns:
            List of BoutRecord objects for Makuuchi bouts that day.
        """
        api_basho = basho_id.replace(".", "")
        url = f"{BASE_URL}/Results_text.aspx?b={api_basho}&d={day}&l=e"

        html = self._fetch_page(url)
        if not html:
            return []

        return self._parse_results_text(html, basho_id, day)

    def fetch_all_results(self, basho_id: str) -> dict[int, list[BoutRecord]]:
        """
        Scrape results for all 15 days of a tournament.

        Returns:
            Dict mapping day number -> list of BoutRecord.
        """
        all_results = {}
        for day in range(1, 16):
            results = self.fetch_day_results(basho_id, day)
            if results:
                all_results[day] = results
                logger.info(f"Scraped {len(results)} bouts for {basho_id} day {day}")
            else:
                logger.info(f"No results for {basho_id} day {day} (may not have happened yet)")
        return all_results

    def fetch_wrestler_page(self, wrestler_id: str) -> Optional[dict]:
        """
        Scrape individual wrestler info page.

        Args:
            wrestler_id: Sumo Reference rikishi ID.

        Returns:
            Dict with wrestler details, or None.
        """
        url = f"{BASE_URL}/Rikishi.aspx?r={wrestler_id}&l=e"
        html = self._fetch_page(url)
        if not html:
            return None
        return self._parse_rikishi_page(html)

    # ------------------------------------------------------------------
    # Parsers
    # ------------------------------------------------------------------

    def _parse_banzuke_text(self, html: str) -> list[WrestlerProfile]:
        """Parse the text-format banzuke page."""
        soup = BeautifulSoup(html, "html.parser")

        # Get the plain text content
        # The text page wraps content in a <pre> or renders as plain text
        text = soup.get_text()
        lines = text.split("\n")

        profiles = []
        in_makuuchi = False
        in_juryo = False

        wrestler_counter = 0

        for line in lines:
            stripped = line.strip()

            # Detect division headers
            if stripped == "Makuuchi":
                in_makuuchi = True
                in_juryo = False
                continue
            elif stripped == "Juryo":
                in_makuuchi = False
                in_juryo = True
                continue
            elif stripped in ("Makushita", "Sandanme", "Jonidan", "Jonokuchi", "Mae-zumo"):
                in_makuuchi = False
                in_juryo = False
                continue

            if not in_makuuchi:
                continue

            # Try to parse the banzuke line
            match = _BANZUKE_LINE_RE.match(stripped)
            if match:
                profile = self._banzuke_match_to_profile(match, wrestler_counter)
                if profile:
                    profiles.append(profile)
                    wrestler_counter += 1
            else:
                # Try a more lenient parse for lines with irregular spacing
                profile = self._parse_banzuke_line_lenient(stripped, wrestler_counter)
                if profile:
                    profiles.append(profile)
                    wrestler_counter += 1

        logger.info(f"Parsed {len(profiles)} Makuuchi wrestlers from banzuke")
        return profiles

    def _banzuke_match_to_profile(
        self, match: re.Match, counter: int
    ) -> Optional[WrestlerProfile]:
        """Convert a regex match from banzuke text to WrestlerProfile."""
        rank_code = match.group(1)
        rank_num = int(match.group(2))
        side = "east" if match.group(3) == "e" else "west"
        shikona = match.group(4)
        # Group 5 is birthplace (+ possibly heya merged in).
        # We extract heya as the last whitespace-separated token before the date.
        birthplace_heya = match.group(5).strip()
        parts = birthplace_heya.split()
        heya = parts[-1] if parts else "Unknown"
        birth_str = match.group(6)
        height = match.group(7)
        weight = match.group(8)

        rank = _RANK_PREFIX_MAP.get(rank_code)
        if rank is None:
            return None

        # For sanyaku ranks, the number is just ordering (1, 2)
        # For maegashira, the number is the actual rank number
        rank_number = rank_num if rank == Rank.MAEGASHIRA else None
        if rank in (Rank.YOKOZUNA, Rank.OZEKI, Rank.SEKIWAKE, Rank.KOMUSUBI):
            rank_number = None  # sanyaku don't use rank numbers in our model

        birth_date = self._parse_date_dmy(birth_str)

        # Use a stable ID: rank+number+side as a temporary ID
        # In production, we'd map to the Sumo Reference rikishi ID
        wrestler_id = f"sr_{shikona.lower().replace(' ', '_')}"

        try:
            return WrestlerProfile(
                wrestler_id=wrestler_id,
                shikona=shikona,
                rank=rank,
                rank_number=rank_number,
                side=side,
                heya=heya,
                height_cm=float(height) if height and height != "NaN" else None,
                weight_kg=float(weight) if weight and weight != "NaN" else None,
                birth_date=birth_date,
                fighting_style=FightingStyle.HYBRID,
            )
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to create profile for {shikona}: {e}")
            return None

    def _parse_banzuke_line_lenient(
        self, line: str, counter: int
    ) -> Optional[WrestlerProfile]:
        """
        Lenient fallback parser for banzuke lines with irregular formatting.
        Splits on whitespace and matches positionally.
        """
        parts = line.split()
        if len(parts) < 7:
            return None

        # First part should be rank code like Y1e, O1w, M15e
        rank_part = parts[0]
        rank_match = re.match(r"^([YOSKM])(\d+)(e|w)$", rank_part)
        if not rank_match:
            return None

        rank_code = rank_match.group(1)
        rank_num = int(rank_match.group(2))
        side = "east" if rank_match.group(3) == "e" else "west"

        rank = _RANK_PREFIX_MAP.get(rank_code)
        if rank is None:
            return None

        shikona = parts[1]

        # Find heya, birth date, height, weight from remaining parts
        # Format: Shikona Birthplace Heya DD.MM.YYYY Height Weight
        heya = "Unknown"
        birth_date = None
        height = None
        weight = None

        for i, part in enumerate(parts[2:], start=2):
            if re.match(r"\d{2}\.\d{2}\.\d{4}", part):
                heya = parts[i - 1] if i > 2 else "Unknown"
                birth_date = self._parse_date_dmy(part)
                if i + 1 < len(parts):
                    try:
                        height = float(parts[i + 1])
                    except ValueError:
                        pass
                if i + 2 < len(parts):
                    try:
                        weight = float(parts[i + 2])
                    except ValueError:
                        pass
                break

        rank_number = rank_num if rank == Rank.MAEGASHIRA else None
        wrestler_id = f"sr_{shikona.lower().replace(' ', '_')}"

        try:
            return WrestlerProfile(
                wrestler_id=wrestler_id,
                shikona=shikona,
                rank=rank,
                rank_number=rank_number,
                side=side,
                heya=heya,
                height_cm=height,
                weight_kg=weight,
                birth_date=birth_date,
                fighting_style=FightingStyle.HYBRID,
            )
        except (ValueError, TypeError):
            return None

    def _parse_results_text(
        self, html: str, basho_id: str, day: int
    ) -> list[BoutRecord]:
        """
        Parse bout results from the text-format results page.

        Results lines look like:
            Asahakuryu vs Sadanoumi
            or with kimarite:
            Onosato oshidashi Hoshoryu
        """
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text()
        lines = text.split("\n")

        results = []
        in_makuuchi = False

        for line in lines:
            stripped = line.strip()

            if stripped == "Makuuchi":
                in_makuuchi = True
                continue
            elif stripped in ("Juryo", "Makushita", "Sandanme", "Jonidan",
                            "Jonokuchi", "Mae-zumo", ""):
                if stripped and stripped != "":
                    in_makuuchi = False
                continue

            if not in_makuuchi:
                continue

            # Try to parse a result line
            record = self._parse_result_line(stripped, basho_id, day)
            if record:
                results.append(record)

        return results

    def _parse_result_line(
        self, line: str, basho_id: str, day: int
    ) -> Optional[BoutRecord]:
        """
        Parse a single result line.

        Format varies:
            "Wrestler1 kimarite Wrestler2" (winner listed first)
            "Wrestler1 vs Wrestler2" (no result yet, just schedule)
        """
        # Skip lines that are just schedule (contain "vs" without kimarite)
        if " vs " in line and line.count(" ") <= 3:
            return None  # This is a torikumi entry, not a result

        # Pattern: "Winner kimarite Loser Day N Sumo Basho..."
        # The text page often appends "Day N Sumo <BashoName> Basho..."
        # Strip the trailing context
        # Clean up trailing "Day X Sumo..." text
        day_pattern = re.search(r"\s+Day\s+\d+\s+Sumo", line)
        if day_pattern:
            line = line[:day_pattern.start()]

        parts = line.strip().split()
        if len(parts) < 3:
            return None

        # The format is: WinnerShikona kimarite LoserShikona
        # The winner is always listed on the east (first) side in results
        # But we need to figure out east/west from context
        winner_shikona = parts[0]
        kimarite = parts[1] if len(parts) >= 3 else None
        loser_shikona = parts[2] if len(parts) >= 3 else parts[1]

        # Use shikona-based IDs (consistent with banzuke parser)
        east_id = f"sr_{winner_shikona.lower()}"
        west_id = f"sr_{loser_shikona.lower()}"
        winner_id = east_id  # Winner is listed first in sumodb results

        try:
            return BoutRecord(
                basho_id=basho_id,
                day=day,
                east_id=east_id,
                west_id=west_id,
                winner_id=winner_id,
                kimarite=kimarite,
            )
        except (ValueError, TypeError) as e:
            logger.debug(f"Failed to parse result line '{line}': {e}")
            return None

    def _parse_rikishi_page(self, html: str) -> Optional[dict]:
        """Parse individual wrestler page for additional details."""
        soup = BeautifulSoup(html, "html.parser")
        # This is a fallback — main data comes from banzuke
        # Extract what we can from the page text
        text = soup.get_text()
        return {"raw_text": text}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _fetch_page(self, url: str) -> Optional[str]:
        """Fetch a page with rate limiting and error handling."""
        self._rate_limit()
        try:
            logger.debug(f"Scraping: {url}")
            resp = self._session.get(url, timeout=15)
            resp.raise_for_status()
            return resp.text
        except requests.exceptions.Timeout:
            logger.warning(f"Scrape timeout: {url}")
            return None
        except requests.exceptions.HTTPError as e:
            logger.warning(f"Scrape HTTP error {e.response.status_code}: {url}")
            if e.response.status_code == 429:
                time.sleep(self._delay_s * 5)
            return None
        except requests.exceptions.RequestException as e:
            logger.warning(f"Scrape failed: {url} — {e}")
            return None

    def _rate_limit(self) -> None:
        """Enforce minimum delay between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self._delay_s:
            time.sleep(self._delay_s - elapsed)
        self._last_request_time = time.time()

    @staticmethod
    def _parse_date_dmy(date_str: str) -> Optional[Date]:
        """Parse DD.MM.YYYY format."""
        try:
            parts = date_str.split(".")
            if len(parts) == 3:
                return Date(int(parts[2]), int(parts[1]), int(parts[0]))
        except (ValueError, IndexError):
            pass
        return None
