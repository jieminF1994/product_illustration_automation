#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from annuity_automation import (
    DEFAULT_TEMPLATE_PATH,
    RECALC_HELPER,
    filter_product_structure,
    load_json,
    run_phase1,
    run_recalc_helper,
    save_csv,
)
from demo_common import DEMO_DIR, configure_demo_logging, ensure_demo_dir


log = logging.getLogger("demo_2_populate_recalc")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Demo 2: populate workbooks and recalculate")
    parser.add_argument(
        "--template",
        type=Path,
        default=DEFAULT_TEMPLATE_PATH,
        help="Path to the illustration tool template workbook.",
    )
    parser.add_argument(
        "--json",
        type=Path,
        default=DEMO_DIR / "product_structure.json",
        help="Path to product_structure.json produced by demo 1.",
    )
    parser.add_argument(
        "--test-cases",
        nargs="+",
        help="Optional list of specific PDF test cases to process.",
    )
    return parser.parse_args()


def main() -> int:
    started_at = time.perf_counter()
    args = parse_args()

    demo_dir = ensure_demo_dir()
    configure_demo_logging(demo_dir / "demo2_run.log")

    workbook_dir = demo_dir / "excel_test_cases"
    workbook_dir.mkdir(parents=True, exist_ok=True)
    error_report = demo_dir / "demo2_error_report.csv"

    log.info("Demo folder: %s", demo_dir)
    log.info("Template: %s", args.template)
    log.info("Input JSON: %s", args.json)
    log.info("Workbook output folder: %s", workbook_dir)

    if not args.template.exists():
        log.error("Template file not found: %s", args.template)
        return 1
    if not args.json.exists():
        log.error("product_structure.json not found: %s", args.json)
        return 1

    product_structure = load_json(args.json)
    try:
        product_structure = filter_product_structure(product_structure, args.test_cases or [])
    except ValueError as exc:
        log.error("%s", exc)
        return 1

    workbook_names = [f"{pdf_name}.xlsx" for pdf_name in product_structure]
    recalc_path = workbook_dir / "recalc_helper.py"
    recalc_path.write_text(RECALC_HELPER, encoding="utf-8")
    log.info("Wrote recalc helper: %s", recalc_path)

    errors = run_phase1(args.template, product_structure, workbook_dir)
    for i, record in enumerate(errors, 1):
        record["number"] = i
    save_csv(errors, error_report, fieldnames=["number", "test_case", "error_message"])
    log.info("Phase 1 population complete. %d error(s).", len(errors))

    try:
        run_recalc_helper(recalc_path, workbook_dir, workbook_names)
    except Exception as exc:
        log.error("Recalculation failed: %s", exc)
        return 2

    log.info("Demo 2 complete. Workbooks saved to %s", workbook_dir)
    log.info("Total processing time: %.2f seconds", time.perf_counter() - started_at)
    return 0


if __name__ == "__main__":
    sys.exit(main())
