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
        list_label.setFont(QFont("Outfit", 12, QFont.Weight.Bold))
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
        self._placeholder.setStyleSheet("color: #999; font-size: 16px; padding: 40px;")
        self._dossier_layout.addWidget(self._placeholder)
        self._dossier_layout.addStretch()

        right_scroll.setWidget(self._dossier_container)
        splitter.addWidget(right_scroll)

        splitter.setSizes([250, 750])
        layout.addWidget(splitter)

    # ── List population ────────────────────────────────────────────

    def _populate_list(self) -> None:
        self._wrestler_list.clear()
        seen_shikona: set[str] = set()
        for w in self._all_wrestlers:
            # Skip duplicates (same shikona, different IDs from stubs)
            if w.shikona in seen_shikona:
                continue
            seen_shikona.add(w.shikona)

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
            kanji_label.setFont(QFont("Outfit", 28, QFont.Weight.Bold))
            kanji_label.setStyleSheet("color: #1A1A1A;")
            name_row.addWidget(kanji_label)

        eng_label = QLabel(w.shikona)
        eng_label.setFont(QFont("Outfit", 20 if w.shikona_jp else 28, QFont.Weight.Bold))
        eng_label.setStyleSheet(f"color: {_RANK_COLORS.get(w.rank, '#333')};")
        name_row.addWidget(eng_label)
        name_row.addStretch()
        bio_layout.addLayout(name_row)

        # Rank
        rank_label = QLabel(w.full_rank)
        rank_label.setFont(QFont("Outfit", 14))
        rank_label.setStyleSheet(f"color: {_RANK_COLORS.get(w.rank, '#333')};")
        bio_layout.addWidget(rank_label)

        # DOB + Age
        if w.birth_date:
            age = _age_from_dob(w.birth_date)
            dob_text = f"DOB: {w.birth_date.strftime('%B %d, %Y')} (age {age})"
        else:
            dob_text = "DOB: Unknown"
        dob_label = QLabel(dob_text)
        dob_label.setFont(QFont("Outfit", 12))
        bio_layout.addWidget(dob_label)

        # Heya
        heya_label = QLabel(f"Heya: {w.heya}")
        heya_label.setFont(QFont("Outfit", 12))
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
        country_label.setFont(QFont("Outfit", 12))
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
            hw_label.setFont(QFont("Outfit", 12))
            bio_layout.addWidget(hw_label)

        # ── Modifier controls (momentum, injury) ───────────────────
        mod_layout = QHBoxLayout()
        mod_layout.setSpacing(4)
        mod_layout.setContentsMargins(0, 8, 0, 0)

        # Momentum
        mom_label = QLabel("Momentum:")
        mom_label.setFont(QFont("Outfit", 12))
        mod_layout.addWidget(mom_label)
        mom_combo = QComboBox()
        mom_combo.setFixedWidth(100)
        mom_combo.setFont(QFont("Outfit", 12))
        mom_combo.addItem("Auto", None)
        from data.models import MomentumState
        for ms in MomentumState:
            mom_combo.addItem(ms.value.title(), ms)
        mom_combo.currentIndexChanged.connect(
            lambda idx, wid=w.wrestler_id: self._on_dossier_modifier_changed(wid, "momentum", mom_combo.currentData())
        )
        mod_layout.addWidget(mom_combo)

        mod_layout.addSpacing(16)

        # Injury
        inj_label = QLabel("Injury:")
        inj_label.setFont(QFont("Outfit", 12))
        mod_layout.addWidget(inj_label)
        from PyQt6.QtWidgets import QDoubleSpinBox
        inj_spin = QDoubleSpinBox()
        inj_spin.setFixedWidth(80)
        inj_spin.setFont(QFont("Outfit", 12))
        inj_spin.setRange(0.0, 1.0)
        inj_spin.setSingleStep(0.1)
        inj_spin.setValue(0.0)
        inj_spin.setDecimals(2)
        inj_spin.valueChanged.connect(
            lambda val, wid=w.wrestler_id: self._on_dossier_modifier_changed(wid, "injury", val)
        )
        mod_layout.addWidget(inj_spin)

        mod_layout.addStretch()

        # Load current values from modifier panel if available
        self._load_current_modifiers(w.wrestler_id, mom_combo, inj_spin)

        bio_layout.addLayout(mod_layout)

        header.addLayout(bio_layout)

        # ── Shikona History ────────────────────────────────────────
        shikona_group = QGroupBox("Shikona History")
        shikona_group.setFont(QFont("Outfit", 10, QFont.Weight.Bold))
        shikona_group.setFixedWidth(200)
        shikona_vbox = QVBoxLayout(shikona_group)
        shikona_vbox.setSpacing(2)
        shikona_vbox.setContentsMargins(8, 12, 8, 8)

        api_id = w.api_id or (int(w.wrestler_id) if w.wrestler_id.isdigit() else None)
        shikona_history = self._fetch_shikona_history(api_id) if api_id else []
        if shikona_history:
            # Filter out the last entry if it's just the short shikona
            # (API often adds a redundant entry with just the ring name)
            filtered = []
            for entry in shikona_history:
                name_en = entry.get("shikonaEn", "")
                # Skip entries where the name has no space (just the short shikona)
                # unless it's the only entry
                if " " in name_en or len(shikona_history) == 1:
                    filtered.append(entry)

            if not filtered:
                filtered = shikona_history  # Fallback: show all if filter removed everything

            for entry in filtered:
                basho = entry.get("bashoId", "")
                name_en = entry.get("shikonaEn", "")
                # Format basho as YYYY-MM
                if len(basho) == 6:
                    basho_fmt = f"{basho[:4]}-{basho[4:]}"
                else:
                    basho_fmt = basho
                row_label = QLabel(f"{basho_fmt}  {name_en}")
                row_label.setFont(QFont("Outfit", 9))
                row_label.setStyleSheet("color: #333;")
                shikona_vbox.addWidget(row_label)
        else:
            shikona_vbox.addWidget(QLabel("No history available"))

        shikona_vbox.addStretch()
        header.addWidget(shikona_group)

        # ── Family ─────────────────────────────────────────────────
        family_group = QGroupBox("Family in Sumo")
        family_group.setFont(QFont("Outfit", 10, QFont.Weight.Bold))
        family_group.setFixedWidth(200)
        family_vbox = QVBoxLayout(family_group)
        family_vbox.setSpacing(2)
        family_vbox.setContentsMargins(8, 12, 8, 8)

        # Load existing family relations
        if self._db:
            relations = self._db.get_family_relations(w.wrestler_id)
        else:
            relations = []

        if relations:
            for rel in relations:
                rel_label = QLabel(f"{rel['relationship'].title()}: {rel['related_name']}")
                rel_label.setFont(QFont("Outfit", 9))
                rel_label.setStyleSheet("color: #333;")
                family_vbox.addWidget(rel_label)
        else:
            no_family = QLabel("None recorded")
            no_family.setFont(QFont("Outfit", 9))
            no_family.setStyleSheet("color: #999;")
            family_vbox.addWidget(no_family)

        family_vbox.addStretch()

        # Add relation button
        add_family_btn = QPushButton("+ Add")
        add_family_btn.setFixedWidth(60)
        add_family_btn.setFont(QFont("Outfit", 8))
        add_family_btn.clicked.connect(lambda: self._on_add_family(w))
        family_vbox.addWidget(add_family_btn)

        header.addWidget(family_group)

        # ── Rank Progression Graph ──────────────────────────────────
        rank_group = QGroupBox("Rank Progression")
        rank_group.setFont(QFont("Outfit", 10, QFont.Weight.Bold))
        rank_group.setMinimumWidth(320)
        rank_vbox = QVBoxLayout(rank_group)
        rank_vbox.setContentsMargins(4, 12, 4, 4)

        if self._db:
            # Fetch all tournament records for this wrestler
            all_recs = self._db.get_tournament_records(w.wrestler_id, limit=50)
            if all_recs and len(all_recs) >= 2:
                rank_widget = self._build_rank_chart(all_recs)
                rank_vbox.addWidget(rank_widget)
            else:
                no_data = QLabel("Insufficient data")
                no_data.setFont(QFont("Outfit", 9))
                no_data.setStyleSheet("color: #999;")
                no_data.setAlignment(Qt.AlignmentFlag.AlignCenter)
                rank_vbox.addWidget(no_data)
        else:
            rank_vbox.addWidget(QLabel("No database"))

        header.addWidget(rank_group)
        header.addStretch()

        header_widget = QWidget()
        header_widget.setLayout(header)
        self._dossier_layout.addWidget(header_widget)

        # ── Style + Kimarite ───────────────────────────────────────
        style_group = QGroupBox("Fighting Style && Techniques")
        style_layout = QHBoxLayout(style_group)

        style_text = w.fighting_style.value.title()
        style_label = QLabel(f"Style: {style_text}")
        style_label.setFont(QFont("Outfit", 22, QFont.Weight.Bold))
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
                kim_text = QLabel(f"{technique.title()}\n({count})")
                kim_text.setAlignment(Qt.AlignmentFlag.AlignCenter)
                kim_text.setFont(QFont("Outfit", 12, QFont.Weight.Bold))
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
        wins_label.setFont(QFont("Outfit", 18, QFont.Weight.Bold))
        wins_label.setStyleSheet("color: #006400;")
        career_layout.addWidget(wins_label)
        career_layout.addWidget(QLabel("W"))

        career_layout.addWidget(QLabel("  —  "))

        losses_label = QLabel(f"{w.career_losses}")
        losses_label.setFont(QFont("Outfit", 18, QFont.Weight.Bold))
        losses_label.setStyleSheet("color: #8B0000;")
        career_layout.addWidget(losses_label)
        career_layout.addWidget(QLabel("L"))

        if w.career_absences:
            career_layout.addWidget(QLabel("  —  "))
            abs_label = QLabel(f"{w.career_absences}")
            abs_label.setFont(QFont("Outfit", 18, QFont.Weight.Bold))
            abs_label.setStyleSheet("color: #666;")
            career_layout.addWidget(abs_label)
            career_layout.addWidget(QLabel("A"))

        career_layout.addWidget(self._make_separator())

        if w.total_yusho:
            yusho_label = QLabel(f"🏆 {w.total_yusho} Makuuchi Yusho")
            yusho_label.setFont(QFont("Outfit", 14, QFont.Weight.Bold))
            yusho_label.setStyleSheet("color: #B8860B;")
            career_layout.addWidget(yusho_label)

        # Win rate
        total = w.career_wins + w.career_losses
        if total > 0:
            rate = w.career_wins / total * 100
            rate_label = QLabel(f"({rate:.1f}%)")
            rate_label.setStyleSheet("color: #666; font-size: 13px;")
            career_layout.addWidget(rate_label)

        career_layout.addWidget(self._make_separator())

        # Years active
        years_text = self._get_years_active(w)
        years_label = QLabel(years_text)
        years_label.setFont(QFont("Outfit", 18, QFont.Weight.Bold))
        years_label.setStyleSheet("color: #333;")
        career_layout.addWidget(years_label)

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
            "border-radius: 6px; padding: 8px; }"
        )
        card.setFixedWidth(180)
        card.setMinimumHeight(300)

        layout = QVBoxLayout(card)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(6)
        layout.setAlignment(Qt.AlignmentFlag.AlignCenter)

        # Row 1: Basho date
        month = rec.basho_id.split(".")[1] if "." in rec.basho_id else ""
        location = _BASHO_LOCATIONS.get(month, "")
        date_label = QLabel(f"{rec.basho_id}")
        date_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        date_label.setFont(QFont("Outfit", 13, QFont.Weight.Bold))
        date_label.setStyleSheet("color: #333;")
        layout.addWidget(date_label)

        # Row 2: Location
        loc_label = QLabel(location or "")
        loc_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        loc_label.setFont(QFont("Outfit", 10))
        loc_label.setStyleSheet("color: #999;")
        layout.addWidget(loc_label)

        # Row 3: Rank shorthand
        rec_rank_val = rec.rank.value if isinstance(rec.rank, Rank) else str(rec.rank)
        rec_side = getattr(rec, "side", None)
        rank_short = self._rank_shorthand(rec_rank_val, rec.rank_number, rec_side)
        rank_label = QLabel(rank_short)
        rank_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        rank_label.setFont(QFont("Outfit", 14, QFont.Weight.Bold))
        rank_label.setStyleSheet("color: #444;")
        layout.addWidget(rank_label)

        # Row 4: Rank change indicator
        arrow_label = QLabel()
        arrow_label.setAlignment(Qt.AlignmentFlag.AlignCenter)

        if prev:
            prev_rank_val = prev.rank.value if isinstance(prev.rank, Rank) else str(prev.rank)
            prev_side = getattr(prev, "side", None)

            curr_key = self._rank_sort_key(rec_rank_val, rec.rank_number, rec_side)
            prev_key = self._rank_sort_key(prev_rank_val, prev.rank_number, prev_side)

            if curr_key < prev_key:
                arrow_label.setText("▲")
                arrow_label.setStyleSheet("color: #006400; font-size: 22px;")
            elif curr_key > prev_key:
                arrow_label.setText("▼")
                arrow_label.setStyleSheet("color: #8B0000; font-size: 22px;")
            else:
                arrow_label.setText("●")
                arrow_label.setStyleSheet("color: #4169E1; font-size: 16px;")
        else:
            arrow_label.setText("")

        layout.addWidget(arrow_label)

        # Row 5: W-L record
        is_full_kyujo = (rec.wins == 0 and rec.losses == 0 and rec.absences > 0)

        if is_full_kyujo:
            record_label = QLabel(f"0-0-{rec.absences}")
            record_label.setStyleSheet("color: #999;")
        else:
            record_label = QLabel(f"{rec.wins}-{rec.losses}")
            if prev:
                if rec.wins > prev.wins:
                    record_label.setStyleSheet("color: #006400;")
                elif rec.wins < prev.wins:
                    record_label.setStyleSheet("color: #8B0000;")
                else:
                    record_label.setStyleSheet("color: #333;")
            else:
                record_label.setStyleSheet("color: #333;")

        record_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        record_label.setFont(QFont("Outfit", 18, QFont.Weight.Bold))
        layout.addWidget(record_label)

        # Row 6: Yusho / kyujo / special prizes
        extra_label = QLabel()
        extra_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        extra_label.setMinimumHeight(40)
        extra_label.setFont(QFont("Outfit", 20))

        if is_full_kyujo:
            extra_label.setText("✕")
            extra_label.setStyleSheet("color: #8B0000; font-size: 20px; font-weight: bold;")
        else:
            extras = []
            # Determine if this is a makuuchi-level tournament
            is_makuuchi = rec_rank_val in ("yokozuna", "ozeki", "sekiwake", "komusubi", "maegashira")

            if rec.is_yusho:
                if is_makuuchi:
                    extras.append("🏆")  # Emperor's Cup
                else:
                    extras.append("✔")  # Lower division championship
            if rec.is_jun_yusho and is_makuuchi:
                extras.append("🥈")  # Jun-yusho (makuuchi only)
            if rec.special_prizes:
                extras.append("⭐")
            if extras:
                text = " ".join(extras)
                if "✔" in text and "🏆" not in text:
                    extra_label.setText(text)
                    extra_label.setStyleSheet("color: #006400; font-size: 22px; font-weight: bold;")
                else:
                    extra_label.setText(text)

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
        header.setFont(QFont("Outfit", 14, QFont.Weight.Bold))
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
            result_item.setFont(QFont("Outfit", -1, QFont.Weight.Bold))
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

    # ── Add family relation dialog ───────────────────────────────

    def _on_add_family(self, wrestler: WrestlerProfile) -> None:
        """Open a dialog to add a family relation."""
        if not self._db:
            return

        dlg = QDialog(self)
        dlg.setWindowTitle(f"Add Family Relation — {wrestler.shikona}")
        dlg.setMinimumWidth(400)
        layout = QVBoxLayout(dlg)

        # Wrestler selector
        layout.addWidget(QLabel("Related wrestler:"))
        search_box = QLineEdit()
        search_box.setPlaceholderText("Search by name...")
        layout.addWidget(search_box)

        wrestler_list = QListWidget()
        wrestler_list.setMaximumHeight(200)
        layout.addWidget(wrestler_list)

        # Populate with all wrestlers
        all_w = self._db.get_all_wrestlers(active_only=False)
        for w in all_w:
            if w.wrestler_id == wrestler.wrestler_id:
                continue
            item = QListWidgetItem(f"{w.shikona} ({w.wrestler_id})")
            item.setData(Qt.ItemDataRole.UserRole, w.wrestler_id)
            wrestler_list.addItem(item)

        def filter_list(text):
            text_lower = text.lower()
            for i in range(wrestler_list.count()):
                item = wrestler_list.item(i)
                item.setHidden(text_lower not in item.text().lower())

        search_box.textChanged.connect(filter_list)

        # Relationship type
        layout.addWidget(QLabel("Relationship:"))
        rel_combo = QComboBox()
        for rel in ["brother", "father", "son", "uncle", "nephew",
                     "grandfather", "grandson", "cousin"]:
            rel_combo.addItem(rel.title(), rel)
        layout.addWidget(rel_combo)

        # Buttons
        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        buttons.accepted.connect(dlg.accept)
        buttons.rejected.connect(dlg.reject)
        layout.addWidget(buttons)

        if dlg.exec() == QDialog.DialogCode.Accepted:
            selected = wrestler_list.currentItem()
            if selected:
                related_id = selected.data(Qt.ItemDataRole.UserRole)
                relationship = rel_combo.currentData()
                self._db.add_family_relation(
                    wrestler.wrestler_id, related_id, relationship
                )
                # Refresh the dossier to show the new relation
                self._display_dossier(wrestler)

    # ── API data fetching ─────────────────────────────────────────

    @staticmethod
    def _fetch_shikona_history(api_id: int) -> list[dict]:
        """Fetch shikona change history from sumo-api.com."""
        try:
            import httpx
            resp = httpx.get(
                f"https://www.sumo-api.com/api/shikonas?rikishiId={api_id}",
                timeout=10.0,
            )
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    # Sort chronologically (oldest first)
                    return sorted(data, key=lambda x: x.get("bashoId", ""))
        except Exception:
            pass
        return []

    @staticmethod
    def _build_rank_chart(records: list) -> QWidget:
        """Build a rank progression chart using QPainter.

        Y-axis: rank tier (Yokozuna at top, lower divisions at bottom)
        X-axis: basho timeline
        """
        from data.models import Rank

        # Sort chronologically
        sorted_recs = sorted(records, key=lambda r: r.basho_id)

        # Build data points: (basho_index, rank_value)
        # Rank value: lower number = higher rank
        rank_tiers = {
            "yokozuna": 1, "ozeki": 2, "sekiwake": 3, "komusubi": 4,
            "maegashira": 5, "juryo": 6, "makushita": 7, "sandanme": 8,
            "jonidan": 9, "jonokuchi": 10,
        }

        points = []
        basho_labels = []
        for i, rec in enumerate(sorted_recs):
            rank_val = rec.rank.value if isinstance(rec.rank, Rank) else str(rec.rank)
            rank_lower = rank_val.lower().split()[0]
            tier = rank_tiers.get(rank_lower, 10)
            # Sub-rank: add fraction for rank number within tier
            num = rec.rank_number or 1
            y = tier + (num - 1) * 0.05  # Small offset for rank number
            points.append((i, y))
            basho_labels.append(rec.basho_id)

        if len(points) < 2:
            label = QLabel("Insufficient data")
            label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            return label

        # Chart dimensions
        chart_w, chart_h = 300, 180
        margin_l, margin_r, margin_t, margin_b = 30, 15, 10, 30

        pixmap = QPixmap(chart_w, chart_h)
        pixmap.fill(QColor("#FAFAF5"))

        painter = QPainter(pixmap)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)

        plot_x = margin_l
        plot_y = margin_t
        plot_w = chart_w - margin_l - margin_r
        plot_h = chart_h - margin_t - margin_b

        # Y range: find min/max tiers present, add padding
        y_vals = [p[1] for p in points]
        y_min = max(0.5, min(y_vals) - 0.5)
        y_max = min(10.5, max(y_vals) + 0.5)

        x_min = 0
        x_max = max(1, len(points) - 1)

        def to_px(x_idx, y_val):
            px_x = plot_x + (x_idx / x_max) * plot_w if x_max > 0 else plot_x
            # Invert Y: lower tier number = higher on chart
            px_y = plot_y + ((y_val - y_min) / (y_max - y_min)) * plot_h
            return int(px_x), int(px_y)

        # Draw axes
        painter.setPen(QPen(QColor("#999"), 1))
        painter.drawLine(plot_x, plot_y, plot_x, plot_y + plot_h)
        painter.drawLine(plot_x, plot_y + plot_h, plot_x + plot_w, plot_y + plot_h)

        # Y-axis labels (rank abbreviations)
        rank_labels = {1: "Y", 2: "O", 3: "S", 4: "K", 5: "M", 6: "J", 7: "Ms"}
        painter.setFont(QFont("Outfit", 7))
        painter.setPen(QColor("#666"))
        for tier, label in rank_labels.items():
            if y_min <= tier <= y_max:
                _, py = to_px(0, tier)
                painter.drawText(4, py + 4, label)
                # Gridline
                painter.setPen(QPen(QColor("#E8E8E8"), 1))
                painter.drawLine(plot_x + 1, py, plot_x + plot_w, py)
                painter.setPen(QColor("#666"))

        # Makuuchi/Juryo division line
        if y_min <= 5.5 <= y_max:
            _, div_y = to_px(0, 5.5)
            painter.setPen(QPen(QColor("#CC0000"), 1, Qt.PenStyle.DashLine))
            painter.drawLine(plot_x, div_y, plot_x + plot_w, div_y)

        # X-axis labels (years from basho IDs)
        painter.setPen(QColor("#666"))
        painter.setFont(QFont("Outfit", 7))
        shown_years = set()
        for i, basho_id in enumerate(basho_labels):
            year = basho_id[:4] if len(basho_id) >= 4 else ""
            if year and year not in shown_years:
                px_x, _ = to_px(i, y_max)
                painter.drawText(px_x - 12, plot_y + plot_h + 14, year)
                shown_years.add(year)

        # Draw line
        painter.setPen(QPen(QColor("#8B0000"), 2))
        prev = None
        for x_idx, y_val in points:
            cx, cy = to_px(x_idx, y_val)
            if prev:
                painter.drawLine(prev[0], prev[1], cx, cy)
            prev = (cx, cy)

        # Draw dots
        painter.setPen(QPen(QColor("#8B0000"), 1))
        painter.setBrush(QBrush(QColor("#B22222")))
        for x_idx, y_val in points:
            cx, cy = to_px(x_idx, y_val)
            painter.drawEllipse(cx - 2, cy - 2, 4, 4)

        painter.end()

        label = QLabel()
        label.setPixmap(pixmap)
        return label

    # ── Helpers ─────────────────────────────────────────────────────

    def _get_years_active(self, w: WrestlerProfile) -> str:
        """Get years active string like '2017 – Present' or '2001 – 2021'."""
        start_year = None
        end_year = None

        # Try debut_basho first (e.g. "201711" or "2017.11")
        if w.debut_basho:
            try:
                start_year = int(str(w.debut_basho).replace(".", "")[:4])
            except (ValueError, IndexError):
                pass

        if self._db:
            # Check tournament records for start/end
            recs = self._db.get_tournament_records(w.wrestler_id, limit=100)
            if recs:
                basho_ids = sorted(r.basho_id for r in recs)
                try:
                    if not start_year:
                        start_year = int(basho_ids[0][:4])
                    end_year = int(basho_ids[-1][:4])
                except (ValueError, IndexError):
                    pass

            # Fall back to bout records if no tournament records
            if not end_year or not start_year:
                bouts = self._db.get_career_bouts(w.wrestler_id)
                if bouts:
                    basho_ids = sorted(set(b.basho_id for b in bouts))
                    try:
                        if not start_year:
                            start_year = int(basho_ids[0][:4])
                        end_year = int(basho_ids[-1][:4])
                    except (ValueError, IndexError):
                        pass

        if not start_year:
            return ""

        if w.is_active:
            return f"{start_year} – Present"
        elif end_year:
            return f"{start_year} – {end_year}"
        else:
            return f"{start_year} –"

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

    # ── Modifier sync with ModifierPanel ───────────────────────────

    def _get_modifier_panel(self):
        """Get the modifier panel from the main window."""
        mw = self._main_window
        if mw and hasattr(mw, "_modifier_panel"):
            return mw._modifier_panel
        return None

    def _load_current_modifiers(self, wrestler_id: str, mom_combo, inj_spin):
        """Load current modifier values from the database, then sync with modifier panel."""
        # First try database for persisted overrides
        if self._db:
            overrides = self._db.get_modifier_overrides()
            if wrestler_id in overrides:
                ov = overrides[wrestler_id]
                # Momentum
                mom_val = ov.get("momentum")
                if mom_val:
                    from data.models import MomentumState
                    try:
                        ms = MomentumState(mom_val)
                        idx = mom_combo.findData(ms)
                        if idx >= 0:
                            mom_combo.setCurrentIndex(idx)
                    except ValueError:
                        pass
                # Injury
                inj_val = ov.get("injury_severity", 0.0)
                if inj_val:
                    inj_spin.setValue(inj_val)
                return

        # Fall back to reading from modifier panel widgets
        panel = self._get_modifier_panel()
        if not panel or not hasattr(panel, "_override_table"):
            return

        table = panel._override_table
        for row in range(table.rowCount()):
            item = table.item(row, 0)
            if item and item.data(Qt.ItemDataRole.UserRole) == wrestler_id:
                mom_widget = table.cellWidget(row, 2)
                if mom_widget:
                    mom_combo.setCurrentIndex(mom_widget.currentIndex())
                inj_widget = table.cellWidget(row, 3)
                if inj_widget:
                    inj_spin.setValue(inj_widget.value())
                break

    def _on_dossier_modifier_changed(self, wrestler_id: str, field: str, value):
        """Sync a modifier change to the modifier panel and save to database."""
        # Sync to modifier panel widgets
        panel = self._get_modifier_panel()
        if panel and hasattr(panel, "_override_table"):
            table = panel._override_table
            for row in range(table.rowCount()):
                item = table.item(row, 0)
                if item and item.data(Qt.ItemDataRole.UserRole) == wrestler_id:
                    if field == "momentum":
                        mom_widget = table.cellWidget(row, 2)
                        if mom_widget:
                            if value is None:
                                mom_widget.setCurrentIndex(0)
                            else:
                                idx = mom_widget.findData(value)
                                if idx >= 0:
                                    mom_widget.setCurrentIndex(idx)
                    elif field == "injury":
                        inj_widget = table.cellWidget(row, 3)
                        if inj_widget:
                            inj_widget.setValue(value)
                    break

        # Save to database
        if self._db:
            # Read current values to do a full save
            overrides = self._db.get_modifier_overrides()
            current = overrides.get(wrestler_id, {"momentum": None, "injury_severity": 0.0})

            if field == "momentum":
                mom_str = value.value if value else None
                current["momentum"] = mom_str
            elif field == "injury":
                current["injury_severity"] = value

            self._db.save_modifier_override(
                wrestler_id,
                current.get("momentum"),
                current.get("injury_severity", 0.0),
            )
