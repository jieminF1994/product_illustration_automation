#!/usr/bin/env python3
from __future__ import annotations

import logging
import sys
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parent
PROJECT_PARENT_DIR = PROJECT_DIR.parent
DEFAULT_PDF_DIR = PROJECT_PARENT_DIR / "data" / "pdf"
DEMO_DIR = PROJECT_DIR / "demo"
LOG_FORMAT = "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"

try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass


def ensure_demo_dir() -> Path:
    DEMO_DIR.mkdir(parents=True, exist_ok=True)
    return DEMO_DIR


def configure_demo_logging(log_file: Path) -> None:
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    formatter = logging.Formatter(LOG_FORMAT)

    has_console = any(getattr(handler, "_demo_console", False) for handler in root.handlers)
    if not has_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler._demo_console = True  # type: ignore[attr-defined]
        root.addHandler(console_handler)

    resolved_log_file = log_file.resolve()
    for handler in list(root.handlers):
        if isinstance(handler, logging.FileHandler):
            existing = Path(getattr(handler, "baseFilename", "")).resolve()
            if existing == resolved_log_file:
                root.removeHandler(handler)
                handler.close()

    file_handler = logging.FileHandler(resolved_log_file, mode="w", encoding="utf-8")
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)
