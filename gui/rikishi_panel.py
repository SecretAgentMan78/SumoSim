"""
SumoSim Rikishi Dossier Panel

A comprehensive fighter profile view showing:
  - Large portrait with kanji name
  - Bio details (DOB, heya, country with flag, height/weight)
  - Fighting style and top 3 kimarite with technique images
  - Career W-L-Kyujo summary
  - Last 5 basho progression graphic with rank changes and records
  - Drilldown to full lifetime bout record
  - Active/All toggle for viewing retired wrestlers
"""

from __future__ import annotations

import logging
import math
import os
from datetime import date
from typing import Optional

from PyQt6.QtCore import Qt, QSize, QUrl
from PyQt6.QtGui import QColor, QFont, QIcon, QPixmap, QPainter, QPen, QBrush
from PyQt6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFrame,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from data.models import (
    BoutRecord,
    FightingStyle,
    Rank,
    TournamentRecord,
    WrestlerProfile,
)

logger = logging.getLogger(__name__)

# ── Rank colors ────────────────────────────────────────────────────
_RANK_COLORS = {
    Rank.YOKOZUNA: "#B8860B",
    Rank.OZEKI: "#8B0000",
    Rank.SEKIWAKE: "#00008B",
    Rank.KOMUSUBI: "#006400",
    Rank.MAEGASHIRA: "#3C3C3C",
}

_RANK_LABELS = {
    Rank.YOKOZUNA: "Y", Rank.OZEKI: "O", Rank.SEKIWAKE: "S",
    Rank.KOMUSUBI: "K", Rank.MAEGASHIRA: "M",
}

# ── Country code mapping for flags ────────────────────────────────
_COUNTRY_CODES = {
    "Japan": "jp", "Mongolia": "mn", "Ukraine": "ua",
    "Kazakhstan": "kz", "Georgia": "ge", "Brazil": "br",
    "Bulgaria": "bg", "Russia": "ru", "Egypt": "eg",
    "Tonga": "to", "China": "cn", "USA": "us",
}

# ── Basho location mapping ────────────────────────────────────────
_BASHO_LOCATIONS = {
    "01": "Tokyo", "03": "Osaka", "05": "Tokyo",
    "07": "Nagoya", "09": "Tokyo", "11": "Fukuoka",
}


def _image_path(subdir: str, filename: str) -> str | None:
    """Resolve an image path from data/images/{subdir}/{filename}."""
    pkg_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for base in [pkg_dir, os.path.dirname(pkg_dir)]:
        path = os.path.join(base, "data", "images", subdir, filename)
        if os.path.isfile(path):
            return path
    return None


def _rikishi_photo(wrestler_id: str, shikona: str = "") -> str | None:
    """Find a rikishi portrait image."""
    for name in [f"{wrestler_id}.jpg", f"{shikona.lower()}.jpg"]:
        path = _image_path("rikishi", name)
        if path:
            return path
    return None


def _cm_to_imperial(cm: float) -> str:
    total_inches = round(cm / 2.54)
    feet = total_inches // 12
    inches = total_inches % 12
    return f"{feet}'{inches}\""


def _kg_to_imperial(kg: float) -> str:
    lbs = round(kg * 2.20462)
    return f"{lbs} lbs"


def _age_from_dob(dob: date) -> int:
    today = date.today()
    return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))


class RikishiDossierPanel(QWidget):
    """
    Rikishi Dossier tab — the fighter profile encyclopedia.

    Layout:
        ┌──────────────────┬───────────────────────────────────────────┐
        │  Fighter List     │  Dossier Card                            │
        │  [Search...]      │  ┌──────┬──────────────────────────────┐ │
        │  [x] Active only  │  │ Photo │ 豊昇龍 Hoshoryu              │ │
        │                   │  │      │ DOB, Heya, Country 🇲🇳       │ │
        │  Y Hoshoryu       │  │      │ 188cm (6'2") / 150kg (330lb) │ │
        │  Y Onosato        │  ├──────┴──────────────────────────────┤ │
        │  O Aonishiki      │  │ Style: Yotsu | yorikiri uwatenage  │ │
        │  O Kotozakura     │  │ Career: 285W-178L-20A | 3 Yusho    │ │
        │  ...              │  ├─────────────────────────────────────┤ │
        │                   │  │ Last 5 Basho Progression            │ │
        │                   │  │ [graphic with arrows and records]   │ │
        │                   │  ├─────────────────────────────────────┤ │
        │                   │  │ [📋 Full Career Record]             │ │
        └──────────────────┴───────────────────────────────────────────┘
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self._main_window = parent
        self._all_wrestlers: list[WrestlerProfile] = []
        self._active_only = True
        self._db = None
        self._build_ui()

    def set_data(self, db) -> None:
        """Set the database reference and populate the wrestler list."""
        self._db = db
        self._refresh_list()

    def _refresh_list(self) -> None:
        """Reload wrestler list from database."""
        if not self._db:
            return
        self._all_wrestlers = self._db.get_all_wrestlers(active_only=self._active_only)
        self._populate_list()

    # ── UI Construction ────────────────────────────────────────────

    def _build_ui(self) -> None:
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        splitter = QSplitter(Qt.Orientation.Horizontal)

        # ── Left: wrestler list ────────────────────────────────────
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(8, 8, 4, 8)

        list_label = QLabel("Rikishi")
        list_label.setFont(QFont("", 12, QFont.Weight.Bold))
        left_layout.addWidget(list_label)

        self._search_box = QLineEdit()
        self._search_box.setPlaceholderText("Search...")
        self._search_box.textChanged.connect(self._filter_list)
        left_layout.addWidget(self._search_box)

        self._active_toggle = QCheckBox("Active wrestlers only")
        self._active_toggle.setChecked(True)
        self._active_toggle.toggled.connect(self._on_active_toggled)
        left_layout.addWidget(self._active_toggle)

        self._wrestler_list = QListWidget()
        self._wrestler_list.setMinimumWidth(220)
        self._wrestler_list.setIconSize(QSize(32, 32))
        self._wrestler_list.currentItemChanged.connect(self._on_wrestler_selected)
        left_layout.addWidget(self._wrestler_list)

        splitter.addWidget(left_panel)

        # ── Right: dossier card ────────────────────────────────────
        right_scroll = QScrollArea()
        right_scroll.setWidgetResizable(True)
        right_scroll.setFrameShape(QFrame.Shape.NoFrame)

        self._dossier_container = QWidget()
        self._dossier_layout = QVBoxLayout(self._dossier_container)
        self._dossier_layout.setContentsMargins(12, 12, 12, 12)
        self._dossier_layout.setSpacing(12)

        # Placeholder
        self._placeholder = QLabel("Select a wrestler to view their dossier")
        self._placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._placeholder.setStyleSheet("color: #999; font-size: 14px; padding: 40px;")
        self._dossier_layout.addWidget(self._placeholder)
        self._dossier_layout.addStretch()

        right_scroll.setWidget(self._dossier_container)
        splitter.addWidget(right_scroll)

        splitter.setSizes([250, 750])
        layout.addWidget(splitter)

    # ── List population ────────────────────────────────────────────

    def _populate_list(self) -> None:
        self._wrestler_list.clear()
        for w in self._all_wrestlers:
            item = QListWidgetItem()
            rl = _RANK_LABELS.get(w.rank, "?")
            num = str(w.rank_number) if w.rank_number else ""
            label = f" {rl}{num}  {w.shikona}"
            if not w.is_active:
                label += "  (引退)"
            item.setText(label)
            item.setData(Qt.ItemDataRole.UserRole, w.wrestler_id)

            # Thumbnail
            photo = _rikishi_photo(w.wrestler_id, w.shikona)
            if photo:
                px = QPixmap(photo).scaledToWidth(32, Qt.TransformationMode.SmoothTransformation)
                if px.height() > 32:
                    px = px.copy(0, 0, 32, 32)
                item.setIcon(QIcon(px))

            color = _RANK_COLORS.get(w.rank, "#3C3C3C")
            item.setForeground(QColor(color))
            self._wrestler_list.addItem(item)

    def _filter_list(self, text: str) -> None:
        text_lower = text.lower()
        for i in range(self._wrestler_list.count()):
            item = self._wrestler_list.item(i)
            visible = text_lower in item.text().lower()
            item.setHidden(not visible)

    def _on_active_toggled(self, checked: bool) -> None:
        self._active_only = checked
        self._refresh_list()

    def _on_wrestler_selected(self, current, previous) -> None:
        if not current:
            return
        wrestler_id = current.data(Qt.ItemDataRole.UserRole)
        wrestler = next(
            (w for w in self._all_wrestlers if w.wrestler_id == wrestler_id), None
        )
        if wrestler:
            self._display_dossier(wrestler)

    # ── Dossier Card ───────────────────────────────────────────────

    def _display_dossier(self, w: WrestlerProfile) -> None:
        """Build and display the full dossier card for a wrestler."""
        # Clear existing content
        while self._dossier_layout.count():
            child = self._dossier_layout.takeAt(0)
            if child.widget():
                child.widget().deleteLater()
            elif child.layout():
                self._clear_layout(child.layout())

        # ── Header: photo + name/bio ───────────────────────────────
        header = QHBoxLayout()

        # Photo
        photo_label = QLabel()
        photo_label.setMinimumSize(200, 250)
        photo_label.setMaximumWidth(250)
        photo_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        photo_label.setStyleSheet(
            "border: 3px solid #8B0000; border-radius: 8px; background-color: #F0EBE0;"
        )
        photo_path = _rikishi_photo(w.wrestler_id, w.shikona)
        if photo_path:
            px = QPixmap(photo_path)
            # Scale to fit within 244x300, keeping aspect ratio — no cropping
            px = px.scaled(
                244, 300,
                Qt.AspectRatioMode.KeepAspectRatio,
                Qt.TransformationMode.SmoothTransformation,
            )
            photo_label.setPixmap(px)
            photo_label.setFixedSize(px.width() + 6, px.height() + 6)
        else:
            photo_label.setFixedSize(200, 250)
            photo_label.setText("No photo")
            photo_label.setStyleSheet(
                photo_label.styleSheet() + " color: #999; font-size: 11px;"
            )
        header.addWidget(photo_label)

        # Name and bio
        bio_layout = QVBoxLayout()
        bio_layout.setSpacing(4)

        # Kanji + English name
        name_row = QHBoxLayout()
        if w.shikona_jp:
            kanji_label = QLabel(w.shikona_jp)
            kanji_label.setFont(QFont("", 28, QFont.Weight.Bold))
            kanji_label.setStyleSheet("color: #1A1A1A;")
            name_row.addWidget(kanji_label)

        eng_label = QLabel(w.shikona)
        eng_label.setFont(QFont("", 20 if w.shikona_jp else 28, QFont.Weight.Bold))
        eng_label.setStyleSheet(f"color: {_RANK_COLORS.get(w.rank, '#333')};")
        name_row.addWidget(eng_label)
        name_row.addStretch()
        bio_layout.addLayout(name_row)

        # Rank
        rank_label = QLabel(w.full_rank)
        rank_label.setFont(QFont("", 14))
        rank_label.setStyleSheet(f"color: {_RANK_COLORS.get(w.rank, '#333')};")
        bio_layout.addWidget(rank_label)

        # DOB + Age
        if w.birth_date:
            age = _age_from_dob(w.birth_date)
            dob_text = f"DOB: {w.birth_date.strftime('%B %d, %Y')} (age {age})"
        else:
            dob_text = "DOB: Unknown"
        dob_label = QLabel(dob_text)
        dob_label.setFont(QFont("", 12))
        bio_layout.addWidget(dob_label)

        # Heya
        heya_label = QLabel(f"Heya: {w.heya}")
        heya_label.setFont(QFont("", 12))
        bio_layout.addWidget(heya_label)

        # Country + flag + prefecture
        country_row = QHBoxLayout()
        country_display = w.country or "Japan"
        if w.prefecture and (w.country == "Japan" or not w.country):
            from_text = f"From: {w.prefecture}, Japan"
        elif w.prefecture:
            from_text = f"From: {w.prefecture}, {w.country}"
        else:
            from_text = f"From: {country_display}"
        country_label = QLabel(from_text)
        country_label.setFont(QFont("", 12))
        country_row.addWidget(country_label)

        # Flag from CDN — resolve country name to code
        # Normalize country: strip leading/trailing whitespace, handle composite shusshin
        country_clean = (w.country or "Japan").strip()
        # If country contains comma (e.g. "Ulaanbaatar, Mongolia"), take last part
        if "," in country_clean:
            country_clean = country_clean.split(",")[-1].strip()
        code = _COUNTRY_CODES.get(country_clean, "jp")
        flag_label = QLabel()
        flag_label.setFixedSize(30, 20)
        try:
            import httpx
            flag_url = f"https://flagcdn.com/w40/{code}.png"
            resp = httpx.get(flag_url, timeout=5.0)
            if resp.status_code == 200:
                px = QPixmap()
                px.loadFromData(resp.content)
                flag_label.setPixmap(px.scaled(30, 20, Qt.AspectRatioMode.KeepAspectRatio,
                                                Qt.TransformationMode.SmoothTransformation))
        except Exception:
            flag_label.setText(f"[{code}]")
        country_row.addWidget(flag_label)
        country_row.addStretch()
        bio_layout.addLayout(country_row)

        # Height/weight
        if w.height_cm and w.weight_kg:
            hw_text = (
                f"{w.height_cm:.0f} cm ({_cm_to_imperial(w.height_cm)}) / "
                f"{w.weight_kg:.0f} kg ({_kg_to_imperial(w.weight_kg)})"
            )
            hw_label = QLabel(hw_text)
            hw_label.setFont(QFont("", 12))
            bio_layout.addWidget(hw_label)

        bio_layout.addStretch()
        header.addLayout(bio_layout, stretch=1)

        header_widget = QWidget()
        header_widget.setLayout(header)
        self._dossier_layout.addWidget(header_widget)

        # ── Style + Kimarite ───────────────────────────────────────
        style_group = QGroupBox("Fighting Style && Techniques")
        style_layout = QHBoxLayout(style_group)

        style_text = w.fighting_style.value.title()
        style_label = QLabel(f"Style: {style_text}")
        style_label.setFont(QFont("", 22, QFont.Weight.Bold))
        style_layout.addWidget(style_label)

        style_layout.addWidget(self._make_separator())

        # Top 3 kimarite
        if self._db:
            top_kim = self._db.get_top_kimarite(w.wrestler_id, n=3)
        else:
            top_kim = []

        if top_kim:
            for technique, count in top_kim:
                kim_frame = QFrame()
                kim_frame.setFixedWidth(270)
                kim_layout = QVBoxLayout(kim_frame)
                kim_layout.setContentsMargins(4, 4, 4, 4)
                kim_layout.setSpacing(4)

                # Technique image
                kim_img = QLabel()
                kim_img.setFixedSize(250, 250)
                kim_img.setAlignment(Qt.AlignmentFlag.AlignCenter)
                img_path = _image_path("kimarite", f"{technique}.jpg")
                if not img_path:
                    img_path = _image_path("kimarite", f"{technique}.png")
                if img_path:
                    px = QPixmap(img_path).scaled(
                        250, 250, Qt.AspectRatioMode.KeepAspectRatio,
                        Qt.TransformationMode.SmoothTransformation
                    )
                    kim_img.setPixmap(px)
                else:
                    kim_img.setText("?")
                    kim_img.setStyleSheet("color: #999; border: 1px solid #ccc; border-radius: 4px;")
                kim_layout.addWidget(kim_img, alignment=Qt.AlignmentFlag.AlignCenter)

                # Technique name + count
                kim_text = QLabel(f"{technique}\n({count})")
                kim_text.setAlignment(Qt.AlignmentFlag.AlignCenter)
                kim_text.setFont(QFont("", 10))
                kim_layout.addWidget(kim_text)

                style_layout.addWidget(kim_frame)
        else:
            style_layout.addWidget(QLabel("No bout data available"))

        style_layout.addStretch()
        self._dossier_layout.addWidget(style_group)

        # ── Career summary ─────────────────────────────────────────
        career_group = QGroupBox("Career Record")
        career_layout = QHBoxLayout(career_group)

        wins_label = QLabel(f"{w.career_wins}")
        wins_label.setFont(QFont("", 18, QFont.Weight.Bold))
        wins_label.setStyleSheet("color: #006400;")
        career_layout.addWidget(wins_label)
        career_layout.addWidget(QLabel("W"))

        career_layout.addWidget(QLabel("  —  "))

        losses_label = QLabel(f"{w.career_losses}")
        losses_label.setFont(QFont("", 18, QFont.Weight.Bold))
        losses_label.setStyleSheet("color: #8B0000;")
        career_layout.addWidget(losses_label)
        career_layout.addWidget(QLabel("L"))

        if w.career_absences:
            career_layout.addWidget(QLabel("  —  "))
            abs_label = QLabel(f"{w.career_absences}")
            abs_label.setFont(QFont("", 18, QFont.Weight.Bold))
            abs_label.setStyleSheet("color: #666;")
            career_layout.addWidget(abs_label)
            career_layout.addWidget(QLabel("A"))

        career_layout.addWidget(self._make_separator())

        if w.total_yusho:
            yusho_label = QLabel(f"🏆 {w.total_yusho} Yusho")
            yusho_label.setFont(QFont("", 14, QFont.Weight.Bold))
            yusho_label.setStyleSheet("color: #B8860B;")
            career_layout.addWidget(yusho_label)

        # Win rate
        total = w.career_wins + w.career_losses
        if total > 0:
            rate = w.career_wins / total * 100
            rate_label = QLabel(f"({rate:.1f}%)")
            rate_label.setStyleSheet("color: #666; font-size: 13px;")
            career_layout.addWidget(rate_label)

        career_layout.addStretch()
        self._dossier_layout.addWidget(career_group)

        # ── Last 5 basho progression ───────────────────────────────
        if self._db:
            # Fetch 6 records: 5 to display + 1 extra for the oldest arrow
            recent = self._db.get_recent_basho_records(w.wrestler_id, n=6)
        else:
            recent = []

        if recent:
            prog_group = QGroupBox("Recent Basho Progression")
            prog_layout = QHBoxLayout(prog_group)
            prog_layout.setSpacing(4)

            # Sort chronologically (oldest first)
            recent_sorted = sorted(recent, key=lambda r: r.basho_id)

            # Display only the last 5, but use the 6th (if available) for
            # the oldest card's arrow
            if len(recent_sorted) > 5:
                sixth = recent_sorted[0]
                display = recent_sorted[1:6]
            else:
                sixth = None
                display = recent_sorted

            prev_record = sixth  # May be None if fewer than 6 records
            for i, rec in enumerate(display):
                basho_widget = self._make_basho_card(rec, prev_record)
                prog_layout.addWidget(basho_widget)
                if i < len(display) - 1:
                    arrow = QLabel("→")
                    arrow.setAlignment(Qt.AlignmentFlag.AlignCenter)
                    arrow.setStyleSheet("color: #999; font-size: 16px;")
                    prog_layout.addWidget(arrow)
                prev_record = rec

            prog_layout.addStretch()
            self._dossier_layout.addWidget(prog_group)

        # ── Full record button ─────────────────────────────────────
        btn_row = QHBoxLayout()
        btn_row.addStretch()
        record_btn = QPushButton("📋  View Full Career Record")
        record_btn.setMinimumWidth(250)
        record_btn.setMinimumHeight(36)
        record_btn.clicked.connect(lambda: self._show_career_record(w))
        btn_row.addWidget(record_btn)
        btn_row.addStretch()
        self._dossier_layout.addLayout(btn_row)

        self._dossier_layout.addStretch()

    # ── Rank shorthand notation ──────────────────────────────────

    _RANK_SHORT = {
        "yokozuna": "Y", "ozeki": "O", "sekiwake": "S",
        "komusubi": "K", "maegashira": "M", "juryo": "J",
        "makushita": "Ms", "sandanme": "Sd",
        "jonidan": "Jd", "jonokuchi": "Jk",
    }

    _SIDE_SHORT = {"east": "E", "west": "W"}

    @classmethod
    def _rank_shorthand(cls, rank_val: str, rank_number: int | None, side: str | None = None) -> str:
        """Convert rank to shorthand: Y1E, O2W, M15E, etc."""
        rank_lower = rank_val.lower().split()[0] if rank_val else "maegashira"
        short = cls._RANK_SHORT.get(rank_lower, rank_lower[0].upper())
        num = str(rank_number) if rank_number else ""
        side_short = cls._SIDE_SHORT.get((side or "").lower(), "")
        return f"{short}{num}{side_short}"

    @staticmethod
    def _rank_sort_key(rank_val: str, rank_number: int | None, side: str | None) -> tuple:
        """Numeric sort key for rank comparison. Lower = higher rank.
        East is considered higher (promoted) vs West at same rank/number."""
        tier_map = {
            "yokozuna": 1, "ozeki": 2, "sekiwake": 3, "komusubi": 4,
            "maegashira": 5, "juryo": 6, "makushita": 7, "sandanme": 8,
            "jonidan": 9, "jonokuchi": 10,
        }
        rank_lower = rank_val.lower().split()[0] if rank_val else "maegashira"
        tier = tier_map.get(rank_lower, 11)
        num = rank_number or 99
        side_val = 0 if (side or "").lower() == "east" else 1
        return (tier, num, side_val)

    # ── Basho progression card ─────────────────────────────────────

    def _make_basho_card(
        self, rec: TournamentRecord, prev: TournamentRecord | None
    ) -> QWidget:
        """Create a single basho card for the progression display."""
        card = QFrame()
        card.setFrameShape(QFrame.Shape.StyledPanel)
        card.setStyleSheet(
            "QFrame { background-color: #FAFAF5; border: 1px solid #D0C8B0; "
            "border-radius: 6px; padding: 6px; }"
        )
        card.setFixedWidth(120)
        card.setMinimumHeight(180)

        layout = QVBoxLayout(card)
        layout.setContentsMargins(6, 6, 6, 6)
        layout.setSpacing(2)

        # Basho date + location in same cell
        month = rec.basho_id.split(".")[1] if "." in rec.basho_id else ""
        location = _BASHO_LOCATIONS.get(month, "")
        date_label = QLabel(f"{rec.basho_id}")
        date_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        date_label.setFont(QFont("", 9, QFont.Weight.Bold))
        date_label.setStyleSheet("color: #333;")
        layout.addWidget(date_label)

        if location:
            loc_label = QLabel(location)
            loc_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            loc_label.setFont(QFont("", 7))
            loc_label.setStyleSheet("color: #999;")
            layout.addWidget(loc_label)

        # Rank shorthand
        rec_rank_val = rec.rank.value if isinstance(rec.rank, Rank) else str(rec.rank)
        # Try to get side from the record if available
        rec_side = getattr(rec, "side", None)
        rank_short = self._rank_shorthand(rec_rank_val, rec.rank_number, rec_side)
        rank_label = QLabel(rank_short)
        rank_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        rank_label.setFont(QFont("", 9, QFont.Weight.Bold))
        rank_label.setStyleSheet("color: #444;")
        layout.addWidget(rank_label)

        # Rank change indicator
        arrow_label = QLabel()
        arrow_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        arrow_label.setFont(QFont("", 14))

        if prev:
            prev_rank_val = prev.rank.value if isinstance(prev.rank, Rank) else str(prev.rank)
            prev_side = getattr(prev, "side", None)

            curr_key = self._rank_sort_key(rec_rank_val, rec.rank_number, rec_side)
            prev_key = self._rank_sort_key(prev_rank_val, prev.rank_number, prev_side)

            if curr_key < prev_key:
                # Promoted
                arrow_label.setText("▲")
                arrow_label.setStyleSheet("color: #006400; font-size: 16px;")
            elif curr_key > prev_key:
                # Demoted
                arrow_label.setText("▼")
                arrow_label.setStyleSheet("color: #8B0000; font-size: 16px;")
            else:
                # Same rank — blue dot
                arrow_label.setText("●")
                arrow_label.setStyleSheet("color: #4169E1; font-size: 12px;")
        else:
            arrow_label.setText("")

        layout.addWidget(arrow_label)

        # W-L record
        record_label = QLabel(f"{rec.wins}-{rec.losses}")
        record_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        record_label.setFont(QFont("", 13, QFont.Weight.Bold))

        # Color based on comparison with previous
        if prev:
            if rec.wins > prev.wins:
                record_label.setStyleSheet("color: #006400;")
            elif rec.wins < prev.wins:
                record_label.setStyleSheet("color: #8B0000;")
            else:
                record_label.setStyleSheet("color: #333;")
        else:
            record_label.setStyleSheet("color: #333;")

        layout.addWidget(record_label)

        # Yusho / special prizes — always present as 6th cell for alignment
        extra_label = QLabel()
        extra_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        extra_label.setFixedHeight(28)
        extra_label.setFont(QFont("", 14))
        extras = []
        if rec.is_yusho:
            extras.append("🏆")
        if rec.is_jun_yusho:
            extras.append("🥈")
        if rec.special_prizes:
            extras.append("⭐")
        if extras:
            extra_label.setText(" ".join(extras))
        layout.addWidget(extra_label)

        return card

    # ── Full career record dialog ──────────────────────────────────

    def _show_career_record(self, w: WrestlerProfile) -> None:
        """Open a dialog showing the full lifetime bout record."""
        if not self._db:
            return

        dlg = QDialog(self)
        dlg.setWindowTitle(f"Career Record — {w.shikona}")
        dlg.resize(800, 600)
        layout = QVBoxLayout(dlg)

        # Header
        header = QLabel(f"{w.shikona}  ({w.full_rank})  —  {w.career_wins}W-{w.career_losses}L")
        header.setFont(QFont("", 14, QFont.Weight.Bold))
        layout.addWidget(header)

        # Table
        table = QTableWidget()
        table.setColumnCount(8)
        table.setHorizontalHeaderLabels([
            "Basho", "Day", "Opponent", "Opp. Rank", "Opp. Heya",
            "Result", "Kimarite", "Opp. Style"
        ])
        th = table.horizontalHeader()
        th.setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        th.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        th.setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)
        table.setAlternatingRowColors(True)
        table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        table.setStyleSheet("alternate-background-color: #F0EDE4;")

        # Load bouts
        bouts = self._db.get_career_bouts(w.wrestler_id)

        # Pre-load opponent names
        opponent_cache: dict[str, str] = {}

        table.setRowCount(len(bouts))
        for row, bout in enumerate(bouts):
            # Determine opponent
            if bout.east_id == w.wrestler_id:
                opp_id = bout.west_id
            else:
                opp_id = bout.east_id

            # Opponent name (cached)
            if opp_id not in opponent_cache:
                opponent_cache[opp_id] = self._db.get_wrestler_name(opp_id)
            opp_name = opponent_cache[opp_id]

            won = bout.winner_id == w.wrestler_id

            table.setItem(row, 0, QTableWidgetItem(bout.basho_id))

            day_item = QTableWidgetItem(str(bout.day))
            day_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            table.setItem(row, 1, day_item)

            table.setItem(row, 2, QTableWidgetItem(opp_name))
            table.setItem(row, 3, QTableWidgetItem(""))  # Opp rank — would need basho-specific lookup
            table.setItem(row, 4, QTableWidgetItem(""))  # Opp heya — would need wrestler lookup
            
            result_item = QTableWidgetItem("WIN" if won else "LOSS")
            result_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            result_item.setForeground(QColor("#006400" if won else "#8B0000"))
            result_item.setFont(QFont("", -1, QFont.Weight.Bold))
            table.setItem(row, 5, result_item)

            table.setItem(row, 6, QTableWidgetItem(bout.kimarite or ""))
            table.setItem(row, 7, QTableWidgetItem(""))  # Opp style

        table.setRowCount(len(bouts))
        layout.addWidget(table)

        # Summary
        wins = sum(1 for b in bouts if b.winner_id == w.wrestler_id)
        losses = len(bouts) - wins
        summary = QLabel(f"Total bouts: {len(bouts)}  |  Wins: {wins}  |  Losses: {losses}")
        summary.setAlignment(Qt.AlignmentFlag.AlignCenter)
        summary.setStyleSheet("padding: 8px; color: #666;")
        layout.addWidget(summary)

        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        buttons.rejected.connect(dlg.close)
        layout.addWidget(buttons)

        dlg.exec()

    # ── Helpers ─────────────────────────────────────────────────────

    @staticmethod
    def _make_separator() -> QFrame:
        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.VLine)
        sep.setFrameShadow(QFrame.Shadow.Sunken)
        sep.setFixedWidth(20)
        return sep

    @staticmethod
    def _clear_layout(layout) -> None:
        while layout.count():
            child = layout.takeAt(0)
            if child.widget():
                child.widget().deleteLater()
            elif child.layout():
                RikishiDossierPanel._clear_layout(child.layout())
