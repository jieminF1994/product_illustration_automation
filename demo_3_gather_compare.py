#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

from annuity_automation import (
    filter_product_structure,
    load_json,
    run_phase2,
    run_phase3,
    save_csv,
    save_json,
)
from demo_common import DEMO_DIR, configure_demo_logging, ensure_demo_dir


log = logging.getLogger("demo_3_gather_compare")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Demo 3: gather report outputs and compare them")
    parser.add_argument(
        "--json",
        type=Path,
        default=DEMO_DIR / "product_structure.json",
        help="Path to product_structure.json produced by demo 1.",
    )
    parser.add_argument(
        "--workbook-dir",
        type=Path,
        default=DEMO_DIR / "excel_test_cases",
        help="Workbook folder produced by demo 2.",
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
    configure_demo_logging(demo_dir / "demo3_run.log")

    tool_output_path = demo_dir / "tool_calc_output.json"
    check_report_path = demo_dir / "check_report.csv"
    error_report_path = demo_dir / "demo3_error_report.csv"

    log.info("Demo folder: %s", demo_dir)
    log.info("Input JSON: %s", args.json)
    log.info("Workbook folder: %s", args.workbook_dir)

    if not args.json.exists():
        log.error("product_structure.json not found: %s", args.json)
        return 1
    if not args.workbook_dir.exists():
        log.error("Workbook folder not found: %s", args.workbook_dir)
        return 1

    product_structure = load_json(args.json)
    try:
        product_structure = filter_product_structure(product_structure, args.test_cases or [])
    except ValueError as exc:
        log.error("%s", exc)
        return 1

    tool_output, errors = run_phase2(args.workbook_dir, product_structure)
    save_json(tool_output, tool_output_path)

    for i, record in enumerate(errors, 1):
        record["number"] = i
    save_csv(errors, error_report_path, fieldnames=["number", "test_case", "error_message"])
    log.info("Phase 2 complete. %d error(s).", len(errors))

    check_records = run_phase3(tool_output, product_structure)
    save_csv(check_records, check_report_path, fieldnames=["number", "test_case", "scenario", "message"])
    log.info("Phase 3 complete. %d check record(s).", len(check_records))

    log.info("Demo 3 complete. tool_calc_output.json saved to %s", tool_output_path)
    log.info("Total processing time: %.2f seconds", time.perf_counter() - started_at)
    return 0


if __name__ == "__main__":
    sys.exit(main())

