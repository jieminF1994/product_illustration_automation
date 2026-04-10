#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

from demo_common import DEFAULT_PDF_DIR, ensure_demo_dir, configure_demo_logging
from extract_annuity_data import AnnuityExtractorPipeline


log = logging.getLogger("demo_1_extract")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Demo 1: PDF extraction to product_structure.json")
    parser.add_argument(
        "--pdf-dir",
        type=Path,
        default=DEFAULT_PDF_DIR,
        help="Directory containing source PDF files.",
    )
    parser.add_argument(
        "--pdf-extractor",
        choices=["auto", "pypdf", "pypdf2", "pdfkit"],
        default="auto",
        help="PDF text extraction backend.",
    )
    return parser.parse_args()


def main() -> int:
    started_at = time.perf_counter()
    args = parse_args()

    demo_dir = ensure_demo_dir()
    configure_demo_logging(demo_dir / "demo1_run.log")

    output_json = demo_dir / "product_structure.json"
    drop_file = demo_dir / "drop_pdf"

    log.info("Demo folder: %s", demo_dir)
    log.info("PDF directory: %s", args.pdf_dir)
    log.info("Output JSON: %s", output_json)

    if not args.pdf_dir.exists():
        log.error("PDF directory not found: %s", args.pdf_dir)
        return 1

    try:
        pipeline = AnnuityExtractorPipeline(
            pdf_dir=args.pdf_dir,
            output_json=output_json,
            drop_file=drop_file,
            extractor_backend=args.pdf_extractor,
        )
    except RuntimeError as exc:
        log.error("Failed to initialize extractor: %s", exc)
        return 2

    _, dropped = pipeline.run()
    log.info("Demo 1 complete. drop_pdf count: %d", len(dropped))
    log.info("Total processing time: %.2f seconds", time.perf_counter() - started_at)
    return 0


if __name__ == "__main__":
    sys.exit(main())

