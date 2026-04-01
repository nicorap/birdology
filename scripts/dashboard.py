#!/usr/bin/env python3
"""
Birdology Desktop Dashboard
PySide6 GUI wrapping all CLI functionality.

    python scripts/dashboard.py
"""
from __future__ import annotations

import datetime
import os
import sys
import webbrowser
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

# Load .env so EBIRD_API_KEY is available for the live check
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

from PySide6.QtCore import Qt, QThread, Signal, QProcess
from PySide6.QtGui import QFont, QTextCursor
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTabWidget, QLabel, QLineEdit, QPushButton, QTableWidget,
    QTableWidgetItem, QHeaderView, QFileDialog, QTextEdit,
    QDoubleSpinBox, QSpinBox, QFormLayout, QStatusBar, QProgressBar,
    QComboBox,
)

# ── Dark stylesheet ────────────────────────────────────────────────────────────

_QSS = """
QMainWindow, QWidget {
    background: #1a1a2e; color: #e0e0e0;
    font-family: "Segoe UI", "SF Pro Text", "Helvetica Neue", sans-serif;
    font-size: 13px;
}
QTabWidget::pane { border: 1px solid #2a2a4a; }
QTabBar::tab {
    background: #12122a; color: #888;
    padding: 8px 20px; border-radius: 4px 4px 0 0; margin-right: 2px;
}
QTabBar::tab:selected { background: #1a1a2e; color: #fff; }
QTabBar::tab:hover    { color: #ccc; }
QLineEdit, QDoubleSpinBox, QSpinBox {
    background: #12122a; color: #e0e0e0;
    border: 1px solid #3a3a5a; border-radius: 4px; padding: 5px 8px;
}
QLineEdit:focus, QDoubleSpinBox:focus, QSpinBox:focus {
    border-color: #26a69a;
}
QPushButton {
    background: #26a69a; color: #fff; border: none;
    border-radius: 4px; padding: 6px 18px; font-weight: bold;
}
QPushButton:hover    { background: #2bbbad; }
QPushButton:disabled { background: #333; color: #666; }
QPushButton[role="secondary"] { background: #3949ab; }
QPushButton[role="secondary"]:hover { background: #3f51b5; }
QPushButton[role="danger"]    { background: #c62828; }
QPushButton[role="danger"]:hover    { background: #d32f2f; }
QTableWidget {
    background: #12122a; color: #e0e0e0;
    gridline-color: #2a2a4a; border: none;
    selection-background-color: #26a69a33;
}
QTableWidget::item:alternate { background: #16162e; }
QHeaderView::section {
    background: #0d0d1f; color: #888; padding: 6px;
    border: none; border-right: 1px solid #2a2a4a; font-weight: bold;
}
QTextEdit {
    background: #0d0d1f; color: #b0bec5;
    border: 1px solid #2a2a4a; border-radius: 4px;
    font-family: "JetBrains Mono", "Menlo", monospace; font-size: 12px;
}
QLabel#hint { color: #666; font-size: 11px; }
QStatusBar  { background: #12122a; color: #666; }
QProgressBar {
    background: #12122a; border: 1px solid #2a2a4a;
    border-radius: 3px; max-height: 5px;
}
QProgressBar::chunk { background: #26a69a; border-radius: 3px; }
#toolbar { background: #12122a; border-bottom: 1px solid #2a2a4a; }
"""

ASSISTENS_LAT = 55.6918
ASSISTENS_LON = 12.5559


def _role(btn: "QPushButton", role: str) -> "QPushButton":
    """Set a CSS role property on a button and force stylesheet re-evaluation."""
    btn.setProperty("role", role)
    btn.style().unpolish(btn)
    btn.style().polish(btn)
    return btn


# ── Generic worker thread ──────────────────────────────────────────────────────

class Worker(QThread):
    result  = Signal(object)
    errored = Signal(str)

    def __init__(self, fn, *args, **kwargs):
        super().__init__()
        self._fn, self._args, self._kwargs = fn, args, kwargs

    def run(self):
        try:
            self.result.emit(self._fn(*self._args, **self._kwargs))
        except Exception as exc:
            self.errored.emit(str(exc))


# ── Table helpers ──────────────────────────────────────────────────────────────

def make_table(headers: list[str]) -> QTableWidget:
    t = QTableWidget(0, len(headers))
    t.setHorizontalHeaderLabels(headers)
    t.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
    t.horizontalHeader().setStretchLastSection(True)
    t.verticalHeader().setVisible(False)
    t.setEditTriggers(QTableWidget.NoEditTriggers)
    t.setSelectionBehavior(QTableWidget.SelectRows)
    t.setAlternatingRowColors(True)
    t.setShowGrid(True)
    return t


def fill_table(table: QTableWidget, rows: list[dict], keys: list[str]) -> None:
    table.setRowCount(0)
    table.setRowCount(len(rows))
    for r, row in enumerate(rows):
        for c, key in enumerate(keys):
            val = row.get(key, "")
            table.setItem(r, c, QTableWidgetItem(str(val) if val else ""))


# ── Search tab ─────────────────────────────────────────────────────────────────

class SearchTab(QWidget):
    status = Signal(str)

    _KEYS  = ["scientificName", "commonNameDa", "commonNameFr", "commonNameEn", "eBirdCode"]
    _HEADS = ["Scientific name", "Danish", "French", "English", "eBird code"]

    def __init__(self):
        super().__init__()
        self.graph = None
        self._worker: Worker | None = None
        lay = QVBoxLayout(self)
        lay.setContentsMargins(16, 16, 16, 8)

        row = QHBoxLayout()
        self._input = QLineEdit(placeholderText="Species name in any language (Rødhals, Rouge-gorge, Robin…)")
        self._input.returnPressed.connect(self._run)
        self._btn = QPushButton("Search")
        self._btn.clicked.connect(self._run)
        row.addWidget(self._input)
        row.addWidget(self._btn)
        lay.addLayout(row)

        self._table = make_table(self._HEADS)
        lay.addWidget(self._table)

    def _run(self):
        from birdology.queries import find_species_by_name
        q = self._input.text().strip()
        if not self.graph or not q:
            return
        self._btn.setEnabled(False)
        self.status.emit(f"Searching '{q}'…")
        self._worker = Worker(find_species_by_name, self.graph, q)
        self._worker.result.connect(self._done)
        self._worker.errored.connect(lambda e: self.status.emit(f"Error: {e}"))
        self._worker.finished.connect(lambda: self._btn.setEnabled(True))
        self._worker.start()

    def _done(self, rows):
        fill_table(self._table, rows, self._KEYS)
        self.status.emit(f"{len(rows)} species found.")


# ── Observations tab ───────────────────────────────────────────────────────────

class ObsTab(QWidget):
    status = Signal(str)

    _KEYS  = ["scientificName", "commonNameDa", "commonNameFr", "date", "count", "locality"]
    _HEADS = ["Scientific", "Danish", "French", "Date", "Count", "Locality"]

    def __init__(self):
        super().__init__()
        self.graph = None
        self._worker: Worker | None = None
        lay = QVBoxLayout(self)
        lay.setContentsMargins(16, 16, 16, 8)

        row = QHBoxLayout()
        self._input = QLineEdit(placeholderText="Filter by name — any language (leave blank for all recent)")
        self._input.returnPressed.connect(self._run)
        self._btn = QPushButton("Get Observations")
        self._btn.clicked.connect(self._run)
        row.addWidget(self._input)
        row.addWidget(self._btn)
        lay.addLayout(row)

        self._table = make_table(self._HEADS)
        lay.addWidget(self._table)

    def _run(self):
        from birdology.queries import recent_danish_observations
        if not self.graph:
            return
        q = self._input.text().strip() or None
        self._btn.setEnabled(False)
        self.status.emit("Fetching observations…")
        self._worker = Worker(recent_danish_observations, self.graph, q)
        self._worker.result.connect(self._done)
        self._worker.errored.connect(lambda e: self.status.emit(f"Error: {e}"))
        self._worker.finished.connect(lambda: self._btn.setEnabled(True))
        self._worker.start()

    def _done(self, rows):
        fill_table(self._table, rows, self._KEYS)
        self.status.emit(f"{len(rows)} observations.")


# ── Nearby tab ─────────────────────────────────────────────────────────────────

class NearbyTab(QWidget):
    status = Signal(str)

    _KEYS  = ["scientificName", "commonNameDa", "commonNameFr", "status", "date", "count", "locality"]
    _HEADS = ["Scientific", "Danish", "French", "IUCN", "Date", "Count", "Locality"]

    def __init__(self):
        super().__init__()
        self.graph = None
        self._worker: Worker | None = None
        lay = QVBoxLayout(self)
        lay.setContentsMargins(16, 16, 16, 8)

        form = QFormLayout()
        self._lat = QDoubleSpinBox(decimals=4, minimum=-90.0, maximum=90.0, singleStep=0.01)
        self._lon = QDoubleSpinBox(decimals=4, minimum=-180.0, maximum=180.0, singleStep=0.01)
        self._rad = QDoubleSpinBox(decimals=1, minimum=0.1, maximum=500.0, singleStep=0.5, suffix=" km")
        self._lat.setValue(ASSISTENS_LAT)
        self._lon.setValue(ASSISTENS_LON)
        self._rad.setValue(2.0)
        form.addRow("Latitude:", self._lat)
        form.addRow("Longitude:", self._lon)
        form.addRow("Radius:", self._rad)
        lay.addLayout(form)

        btns = QHBoxLayout()
        preset = _role(QPushButton("📍 Assistens Kirkegård"), "secondary")
        preset.clicked.connect(lambda: (self._lat.setValue(ASSISTENS_LAT), self._lon.setValue(ASSISTENS_LON)))
        self._btn = QPushButton("Find Nearby Birds")
        self._btn.clicked.connect(self._run)
        btns.addWidget(preset)
        btns.addStretch()
        btns.addWidget(self._btn)
        lay.addLayout(btns)

        self._table = make_table(self._HEADS)
        lay.addWidget(self._table)

    def _run(self):
        from birdology.queries import nearby_watch
        if not self.graph:
            return
        lat, lon, r = self._lat.value(), self._lon.value(), self._rad.value()
        self._btn.setEnabled(False)
        self.status.emit(f"Searching within {r} km of ({lat:.4f}, {lon:.4f})…")
        self._worker = Worker(nearby_watch, self.graph, lat, lon, r)
        self._worker.result.connect(self._done)
        self._worker.errored.connect(lambda e: self.status.emit(f"Error: {e}"))
        self._worker.finished.connect(lambda: self._btn.setEnabled(True))
        self._worker.start()

    def _done(self, rows):
        fill_table(self._table, rows, self._KEYS)
        self.status.emit(f"{len(rows)} species nearby.")


# ── Map tab ────────────────────────────────────────────────────────────────────

class MapTab(QWidget):
    status = Signal(str)

    def __init__(self, out_dir: Path):
        super().__init__()
        self.graph = None
        self._out_dir = out_dir
        self._worker: Worker | None = None
        lay = QVBoxLayout(self)
        lay.setContentsMargins(16, 16, 16, 8)

        form = QFormLayout()
        self._species = QLineEdit(placeholderText="e.g. Rødhals  (leave blank for all)")
        self._family  = QLineEdit(placeholderText="e.g. Turdidae")
        self._order   = QLineEdit(placeholderText="e.g. Passeriformes")
        form.addRow("Species:", self._species)
        form.addRow("Family:",  self._family)
        form.addRow("Order:",   self._order)
        lay.addLayout(form)

        self._btn = QPushButton("🗺  Generate & Open Map")
        self._btn.clicked.connect(self._run)
        lay.addWidget(self._btn)

        self._info = QLabel("Configure filters above, then click Generate.")
        self._info.setObjectName("hint")
        self._info.setWordWrap(True)
        lay.addWidget(self._info)
        lay.addStretch()

    def _run(self):
        from visualize import render_map
        if not self.graph:
            return
        sp  = self._species.text().strip() or None
        fa  = self._family.text().strip()  or None
        ord_ = self._order.text().strip()  or None
        label    = (sp or fa or ord_ or "map").replace(" ", "_")
        out_path = self._out_dir / f"birdology_{label}_map.html"

        self._btn.setEnabled(False)
        self._info.setText("Generating…")
        self.status.emit("Generating Leaflet map…")
        self._worker = Worker(render_map, self.graph, out_path, sp, fa, ord_)
        self._worker.result.connect(lambda _: self._open(out_path))
        self._worker.errored.connect(self._err)
        self._worker.finished.connect(lambda: self._btn.setEnabled(True))
        self._worker.start()

    def _open(self, path: Path):
        self._info.setText(f"Saved → {path}")
        self.status.emit(f"Map ready — {path.name}")
        webbrowser.open(path.resolve().as_uri())

    def _err(self, msg: str):
        self._info.setText(f"Error: {msg}")
        self.status.emit(f"Map error: {msg}")


# ── Summary tab ────────────────────────────────────────────────────────────────

class SummaryTab(QWidget):
    status = Signal(str)

    _KEYS = ["orders", "families", "genera", "species", "observations"]

    def __init__(self):
        super().__init__()
        self.graph = None
        self._worker: Worker | None = None
        lay = QVBoxLayout(self)
        lay.setContentsMargins(16, 16, 16, 8)

        self._lbls: dict[str, QLabel] = {}
        form = QFormLayout()
        big = QFont()
        big.setPointSize(20)
        big.setBold(True)
        for key in self._KEYS:
            lbl = QLabel("—")
            lbl.setFont(big)
            lbl.setAlignment(Qt.AlignRight)
            form.addRow(key.capitalize() + ":", lbl)
            self._lbls[key] = lbl
        lay.addLayout(form)

        self._btn = QPushButton("Refresh")
        self._btn.clicked.connect(self._run)
        lay.addWidget(self._btn, alignment=Qt.AlignLeft)
        lay.addStretch()

    def refresh(self):
        self._run()

    def _run(self):
        from birdology.queries import taxonomy_summary
        if not self.graph:
            return
        self._btn.setEnabled(False)
        self._worker = Worker(taxonomy_summary, self.graph)
        self._worker.result.connect(self._done)
        self._worker.finished.connect(lambda: self._btn.setEnabled(True))
        self._worker.start()

    def _done(self, counts: dict):
        for key, lbl in self._lbls.items():
            lbl.setText(f"{counts.get(key, 0):,}")
        self.status.emit("Summary updated.")


# ── Now tab ───────────────────────────────────────────────────────────────────

_MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]

_MIG_EMOJI = {
    "Resident":       "🏠",
    "SummerVisitor":  "☀️",
    "WinterVisitor":  "❄️",
    "PassageMigrant": "🌊",
    "PartialMigrant": "↔️",
}


class NowTab(QWidget):
    """Shows species expected this month + live eBird check."""
    status = Signal(str)

    _KEYS  = ["migStatus", "scientificName", "commonNameDa", "commonNameFr",
              "commonNameEn", "status", "eBirdCode", "liveeBird"]
    _HEADS = ["Migration", "Scientific", "Danish", "French",
              "English", "IUCN", "eBird code", "Live in DK?"]

    def __init__(self):
        super().__init__()
        self.graph = None
        self._worker: Worker | None = None
        self._live_worker: Worker | None = None
        self._live_codes: set[str] = set()

        lay = QVBoxLayout(self)
        lay.setContentsMargins(16, 16, 16, 8)

        # Month selector + run button
        top = QHBoxLayout()
        top.addWidget(QLabel("Month:"))
        self._month_cb = QComboBox()
        for m in _MONTH_NAMES:
            self._month_cb.addItem(m)
        self._month_cb.setCurrentIndex(datetime.date.today().month - 1)
        top.addWidget(self._month_cb)
        top.addSpacing(16)
        self._btn = QPushButton("Show Expected Species")
        self._btn.clicked.connect(self._run)
        top.addWidget(self._btn)
        top.addSpacing(16)
        self._ebird_key = QLineEdit(placeholderText="eBird API key (optional — for live check)")
        self._ebird_key.setText(os.environ.get("EBIRD_API_KEY", ""))
        self._ebird_key.setEchoMode(QLineEdit.Password)
        top.addWidget(self._ebird_key, 1)
        self._live_btn = _role(QPushButton("🔴 Live eBird"), "secondary")
        self._live_btn.clicked.connect(self._fetch_live)
        top.addWidget(self._live_btn)
        lay.addLayout(top)

        self._info = QLabel("")
        self._info.setObjectName("hint")
        lay.addWidget(self._info)

        self._table = make_table(self._HEADS)
        lay.addWidget(self._table)
        self._rows: list[dict] = []

    def _run(self):
        from birdology.queries import currently_present
        if not self.graph:
            return
        month = self._month_cb.currentIndex() + 1
        self._btn.setEnabled(False)
        self.status.emit(f"Finding species present in {_MONTH_NAMES[month-1]}…")
        self._worker = Worker(currently_present, self.graph, month)
        self._worker.result.connect(self._done)
        self._worker.errored.connect(lambda e: self.status.emit(f"Error: {e}"))
        self._worker.finished.connect(lambda: self._btn.setEnabled(True))
        self._worker.start()

    def _done(self, rows: list[dict]):
        self._rows = rows
        self._render()
        month = self._month_cb.currentIndex() + 1
        self.status.emit(f"{len(rows)} species expected in {_MONTH_NAMES[month-1]}.")
        if not rows:
            self._info.setText(
                "No migration data found — run the reasoner first: python scripts/reason.py"
            )
        else:
            self._info.setText("")

    def _fetch_live(self):
        from birdology.ingestion.ebird import fetch_recent_denmark
        key = self._ebird_key.text().strip()
        if not key:
            self.status.emit("Enter an eBird API key to use the live check.")
            return
        self._live_btn.setEnabled(False)
        self.status.emit("Fetching live eBird observations for Denmark…")
        self._live_worker = Worker(fetch_recent_denmark, key, 30)
        self._live_worker.result.connect(self._live_done)
        self._live_worker.errored.connect(self._live_err)
        self._live_worker.finished.connect(lambda: self._live_btn.setEnabled(True))
        self._live_worker.start()

    def _live_done(self, records: list[dict]):
        self._live_codes = {r.get("speciesCode", "") for r in records}
        self._render()
        self.status.emit(f"Live eBird: {len(self._live_codes)} species reported in Denmark (last 30 days).")

    def _live_err(self, msg: str):
        self.status.emit(f"eBird live error: {msg}")

    def _render(self):
        """Fill table, annotating live eBird status if available."""
        rows = []
        for r in self._rows:
            code   = r.get("eBirdCode", "")
            status_raw = r.get("migStatus", "")
            r2 = dict(r)
            r2["migStatus"] = _MIG_EMOJI.get(status_raw, "❓") + "  " + status_raw
            if self._live_codes:
                r2["liveeBird"] = "✅ Yes" if code in self._live_codes else "—"
            else:
                r2["liveeBird"] = ""
            rows.append(r2)
        fill_table(self._table, rows, self._KEYS)


# ── Build tab ──────────────────────────────────────────────────────────────────

class BuildTab(QWidget):
    status = Signal(str)

    def __init__(self, root: Path):
        super().__init__()
        self._root = root
        self._proc: QProcess | None = None
        lay = QVBoxLayout(self)
        lay.setContentsMargins(16, 16, 16, 8)

        form = QFormLayout()
        self._dof_max = QSpinBox(minimum=100, maximum=100_000, singleStep=1000, suffix=" records")
        self._dof_max.setValue(5000)
        form.addRow("DOF max records:", self._dof_max)
        lay.addLayout(form)

        btns = QHBoxLayout()
        self._build_btn = QPushButton("▶  Build Graph")
        self._build_btn.clicked.connect(self._build)
        self._reason_btn = _role(QPushButton("⚙  Run Reasoner"), "secondary")
        self._reason_btn.clicked.connect(self._reason)
        self._stop_btn = _role(QPushButton("■  Stop"), "danger")
        self._stop_btn.clicked.connect(self._stop)
        self._stop_btn.setEnabled(False)
        btns.addWidget(self._build_btn)
        btns.addWidget(self._reason_btn)
        btns.addStretch()
        btns.addWidget(self._stop_btn)
        lay.addLayout(btns)

        self._log = QTextEdit(readOnly=True)
        lay.addWidget(self._log)

    def _build(self):
        self._launch([
            sys.executable,
            str(self._root / "scripts" / "build_graph.py"),
            "--dof-max", str(self._dof_max.value()),
        ])

    def _reason(self):
        self._launch([sys.executable, str(self._root / "scripts" / "reason.py")])

    def _launch(self, cmd: list[str]):
        if self._proc and self._proc.state() != QProcess.NotRunning:
            return
        self._log.clear()
        self._proc = QProcess()
        self._proc.setWorkingDirectory(str(self._root))
        self._proc.readyReadStandardOutput.connect(self._read_out)
        self._proc.readyReadStandardError.connect(self._read_err)
        self._proc.finished.connect(self._finished)
        self._proc.start(cmd[0], cmd[1:])
        self._build_btn.setEnabled(False)
        self._reason_btn.setEnabled(False)
        self._stop_btn.setEnabled(True)
        self.status.emit(f"Running: {Path(cmd[1]).name} …")

    def _read_out(self):
        self._append(bytes(self._proc.readAllStandardOutput()).decode("utf-8", errors="replace"), "#b0bec5")

    def _read_err(self):
        self._append(bytes(self._proc.readAllStandardError()).decode("utf-8", errors="replace"), "#ef9a9a")

    def _append(self, text: str, color: str):
        escaped = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\n", "<br>")
        self._log.moveCursor(QTextCursor.End)
        self._log.insertHtml(f'<span style="color:{color}">{escaped}</span>')
        self._log.ensureCursorVisible()

    def _finished(self, code: int, _status):
        self._build_btn.setEnabled(True)
        self._reason_btn.setEnabled(True)
        self._stop_btn.setEnabled(False)
        ok = code == 0
        self._append(f"\n{'✓ Done' if ok else f'✗ Exited {code}'}\n", "#a5d6a7" if ok else "#ef9a9a")
        self.status.emit("Done." if ok else f"Exited {code}.")

    def _stop(self):
        if self._proc:
            self._proc.kill()


# ── Main window ────────────────────────────────────────────────────────────────

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Birdology")
        self.resize(1150, 760)
        self._out_dir = ROOT / "output"
        self._load_worker: Worker | None = None

        # Status bar
        sb = QStatusBar()
        self._progress = QProgressBar()
        self._progress.setRange(0, 0)
        self._progress.setFixedWidth(120)
        self._progress.setVisible(False)
        sb.addPermanentWidget(self._progress)
        self.setStatusBar(sb)
        self._sb = sb

        # Central
        central = QWidget()
        self.setCentralWidget(central)
        root_lay = QVBoxLayout(central)
        root_lay.setContentsMargins(0, 0, 0, 0)
        root_lay.setSpacing(0)

        # Toolbar
        toolbar = QWidget()
        toolbar.setObjectName("toolbar")
        tb = QHBoxLayout(toolbar)
        tb.setContentsMargins(12, 8, 12, 8)
        self._path_edit = QLineEdit(str(ROOT / "output" / "birdology_reasoned.ttl"))
        self._path_edit.setMinimumWidth(380)
        browse = _role(QPushButton("Browse…"), "secondary")
        browse.clicked.connect(self._browse)
        self._load_btn = QPushButton("Load Graph")
        self._load_btn.clicked.connect(self._load)
        tb.addWidget(QLabel("Graph:"))
        tb.addWidget(self._path_edit, 1)
        tb.addWidget(browse)
        tb.addWidget(self._load_btn)
        root_lay.addWidget(toolbar)

        # Tabs
        self._tabs = QTabWidget()
        self._search  = SearchTab()
        self._obs     = ObsTab()
        self._nearby  = NearbyTab()
        self._now     = NowTab()
        self._map     = MapTab(self._out_dir)
        self._summary = SummaryTab()
        self._build   = BuildTab(ROOT)

        for tab, icon, label in [
            (self._search,  "🔍", "Search"),
            (self._obs,     "📋", "Observations"),
            (self._nearby,  "📍", "Nearby"),
            (self._now,     "🗓", "Now"),
            (self._map,     "🗺", "Map"),
            (self._summary, "📊", "Summary"),
            (self._build,   "⚙", "Build"),
        ]:
            self._tabs.addTab(tab, f"{icon}  {label}")

        for tab in [self._search, self._obs, self._nearby, self._now,
                    self._map, self._summary, self._build]:
            tab.status.connect(self._sb.showMessage)

        root_lay.addWidget(self._tabs)

        # Auto-load if the default file exists
        if Path(self._path_edit.text()).exists():
            self._load()

    def _browse(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Open graph", str(ROOT / "output"), "Turtle (*.ttl)"
        )
        if path:
            self._path_edit.setText(path)

    def _load(self):
        from birdology.graph import load_graph
        path = self._path_edit.text().strip()
        if not Path(path).exists():
            self._sb.showMessage(f"File not found: {path}")
            return
        self._load_btn.setEnabled(False)
        self._progress.setVisible(True)
        self._sb.showMessage(f"Loading {Path(path).name}…")
        self._load_worker = Worker(load_graph, path)
        self._load_worker.result.connect(self._graph_loaded)
        self._load_worker.errored.connect(self._load_error)
        self._load_worker.start()

    def _graph_loaded(self, g):
        for tab in [self._search, self._obs, self._nearby, self._now,
                    self._map, self._summary]:
            tab.graph = g
        self._sb.showMessage(f"Loaded — {len(g):,} triples.")
        self._progress.setVisible(False)
        self._load_btn.setEnabled(True)
        self._summary.refresh()

    def _load_error(self, msg: str):
        self._sb.showMessage(f"Load error: {msg}")
        self._progress.setVisible(False)
        self._load_btn.setEnabled(True)


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    app = QApplication(sys.argv)
    app.setApplicationName("Birdology")
    app.setStyleSheet(_QSS)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
