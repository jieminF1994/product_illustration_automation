#!/usr/bin/env python3
"""
Extract annuity illustration data from PDF files into product_structure JSON.

PDF text extraction backends:
- pure Python via pypdf / PyPDF2 (cross-platform)
- macOS PDFKit helper binary fallback
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tempfile
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple


PDF_HELPER_SOURCE = r"""
#import <Foundation/Foundation.h>
#import <PDFKit/PDFKit.h>

int main(int argc, const char * argv[]) {
    @autoreleasepool {
        if (argc < 2) {
            fprintf(stderr, "Usage: pdf_text_helper <pdf_path>\n");
            return 1;
        }
        NSString *path = [NSString stringWithUTF8String:argv[1]];
        NSURL *url = [NSURL fileURLWithPath:path];
        PDFDocument *doc = [[PDFDocument alloc] initWithURL:url];
        if (!doc) {
            fprintf(stderr, "Failed to open PDF\n");
            return 2;
        }
        printf("PAGES:%ld\n", (long)doc.pageCount);
        for (NSInteger i = 0; i < doc.pageCount; i++) {
            PDFPage *page = [doc pageAtIndex:i];
            NSString *text = page.string ?: @"";
            printf("===PAGE:%ld===\n", (long)(i + 1));
            printf("%s\n", [text UTF8String]);
        }
    }
    return 0;
}
"""


ROW_RE = re.compile(
    r"^(?:At Issue\s+\d{1,3}-?\b|(?:[1-9]|[12][0-9]|30)\s+\d{1,3}-?\b)"
)


def normalize_text(value: str) -> str:
    value = unicodedata.normalize("NFKC", value)
    value = value.replace("\u2013", "-").replace("\u2014", "-").replace("\u2212", "-")
    value = value.replace("’", "'")
    value = re.sub(r"[^\x20-\x7E]", " ", value)
    value = re.sub(r"(?<=\d)\s*%", "%", value)
    value = re.sub(r"\$\s+", "$", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def sanitize_column_name(name: str) -> str:
    name = normalize_text(name)
    name = re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_")
    name = re.sub(r"_+", "_", name)
    return name or "Unknown_Column"


def canonical_column_name(raw_name: str) -> str:
    lowered = normalize_text(raw_name).lower()
    lowered = lowered.replace("contract s", "contract")
    lowered = lowered.replace("hypothetical ", "")

    if lowered == "year":
        return "Year"
    if lowered == "age":
        return "Age"
    if "credited interest rate" in lowered:
        return "Credited_Interest_Rate"
    if "interest earned" in lowered:
        return "Interest_Earned"
    if "contract" in lowered and "anniversary" in lowered and "value" in lowered:
        return "Contract_Anniversary_Value"
    if "minimum" in lowered and "accumulation" in lowered and "value" in lowered:
        return "Minimum_Accumulation_Value"
    if "cash" in lowered and "surrender" in lowered and "value" in lowered:
        return "Cash_Surrender_Value"
    if "income base" in lowered:
        return "Income_Base"
    if "annual income" in lowered and "life" in lowered:
        return "Annual_Income_for_Life"
    if "guaranteed lifetime income amount" in lowered:
        return "Guaranteed_Lifetime_Income_Amount"
    if "withdrawal amount" in lowered:
        return "Withdrawal_Amount"
    if "guaranteed return of premium grop" in lowered:
        return "Guaranteed_Return_of_Premium_GROP"
    if "overlay value" in lowered:
        return "Overlay_Value"
    if "income credit" in lowered:
        return "Income_Credit"
    if "cumulative withdrawal" in lowered:
        return "Cumulative_Withdrawal"
    if "death benefit" in lowered:
        return "Death_Benefit"
    if "change" in lowered:
        return sanitize_column_name(raw_name)
    return sanitize_column_name(raw_name)


def dedupe_columns(columns: List[str]) -> List[str]:
    counts: Dict[str, int] = {}
    output: List[str] = []
    for col in columns:
        counts[col] = counts.get(col, 0) + 1
        if counts[col] == 1:
            output.append(col)
        else:
            output.append(f"{col}_{counts[col]}")
    return output


@dataclass
class ScenarioData:
    columns: List[str]
    rows: List[Dict[str, str]]


class PDFKitTextExtractor:
    def __init__(self, helper_dir: Path) -> None:
        self.helper_dir = helper_dir
        self.helper_dir.mkdir(parents=True, exist_ok=True)
        self.helper_source = self.helper_dir / "pdf_text_helper.m"
        self.helper_bin = self.helper_dir / "pdf_text_helper"

    def ensure_helper(self) -> None:
        self.helper_source.write_text(PDF_HELPER_SOURCE, encoding="utf-8")
        compile_cmd = [
            "clang",
            "-fobjc-arc",
            "-framework",
            "Foundation",
            "-framework",
            "PDFKit",
            str(self.helper_source),
            "-o",
            str(self.helper_bin),
        ]
        subprocess.run(compile_cmd, check=True, capture_output=True, text=True)

    def extract_pages(self, pdf_path: Path) -> List[List[str]]:
        if not self.helper_bin.exists():
            self.ensure_helper()

        run = subprocess.run(
            [str(self.helper_bin), str(pdf_path)],
            check=True,
            capture_output=True,
            text=True,
        )
        raw = run.stdout
        chunks = re.split(r"^===PAGE:(\d+)===\s*$", raw, flags=re.MULTILINE)
        pages: List[List[str]] = []
        for idx in range(1, len(chunks), 2):
            page_text = chunks[idx + 1] if idx + 1 < len(chunks) else ""
            lines = [normalize_text(line) for line in page_text.splitlines()]
            lines = [line for line in lines if line]
            pages.append(lines)
        return pages


class PythonPDFTextExtractor:
    def __init__(self, backend: str = "auto") -> None:
        self.backend = backend
        self.engine_name = ""
        self.reader_cls = self._load_reader()

    def _load_reader(self):
        if self.backend in {"auto", "pypdf"}:
            try:
                from pypdf import PdfReader  # type: ignore

                self.engine_name = "pypdf"
                return PdfReader
            except Exception:
                if self.backend == "pypdf":
                    raise
        if self.backend in {"auto", "pypdf2"}:
            try:
                from PyPDF2 import PdfReader  # type: ignore

                self.engine_name = "PyPDF2"
                return PdfReader
            except Exception:
                if self.backend == "pypdf2":
                    raise
        raise RuntimeError(
            "No Python PDF reader available. Install 'pypdf' (recommended) or 'PyPDF2'."
        )

    def extract_pages(self, pdf_path: Path) -> List[List[str]]:
        reader = self.reader_cls(str(pdf_path))
        pages: List[List[str]] = []
        for page in reader.pages:
            text = page.extract_text() or ""
            lines = [normalize_text(line) for line in text.splitlines()]
            lines = [line for line in lines if line]
            pages.append(lines)
        return pages


def build_text_extractor(
    backend: str = "auto", helper_dir: Optional[Path] = None
):
    backend = backend.lower().strip()
    errors: List[str] = []

    if backend in {"auto", "pypdf", "pypdf2"}:
        try:
            python_backend = "auto" if backend == "auto" else backend
            return PythonPDFTextExtractor(backend=python_backend)
        except Exception as exc:
            errors.append(f"python extractor: {exc}")
            if backend in {"pypdf", "pypdf2"}:
                raise RuntimeError("; ".join(errors))

    if backend in {"auto", "pdfkit"}:
        if sys.platform != "darwin":
            msg = "pdfkit backend is only supported on macOS."
            errors.append(msg)
            if backend == "pdfkit":
                raise RuntimeError(msg)
        else:
            temp_root = helper_dir or (Path(tempfile.gettempdir()) / "annuity_pdf_helper")
            return PDFKitTextExtractor(helper_dir=temp_root)

    raise RuntimeError(
        "Could not initialize any PDF extractor backend. "
        + " | ".join(errors)
        + " | On Windows/Linux: pip install pypdf"
    )


class SectionParser:
    PROFILE_FIELDS = {
        "owner": "Owner",
        "issueage": "Issue_Age",
        "ageatissue": "Issue_Age",
        "state": "State",
        "solicitationstate": "State",
        "contracttype": "Contract_Type",
        "product": "Product",
        "productname": "Product",
        "premium": "Premium",
        "initialpremium": "Premium",
    }

    INCOME_FIELDS = {
        "livingbenefit": "Living_Benefit",
        "withdrawaltype": "Withdrawal_Type",
        "ageatactivation": "Income_Start_Age",
        "ageatactivationdate": "Income_Start_Age",
        "ageatlifetimeincomeactivation": "Income_Start_Age",
        "ageat1st": "Income_Start_Age",
        "ageat1stwithdrawal": "Income_Start_Age",
        "ageatfirstwithdrawal": "Income_Start_Age",
        "incomestartage": "Income_Start_Age",
        "withdrawalrate": "Initial_Income_Rate",
        "incomepercentageincrease": "Income_Percentage_Increase",
        "guaranteedgrowthrate": "Guaranteed_Growth_Rate",
        "withdrawalfrequency": "Withdrawal_Frequency",
        "annualriderfee": "Annual_Rider_Fee",
        "riderfee": "Annual_Rider_Fee",
    }

    @classmethod
    def parse_sections(cls, lines: List[str]) -> Tuple[Dict[str, str], Dict[str, str], Dict[str, List[str]]]:
        profile = {
            "Owner": "",
            "Issue_Age": "",
            "State": "",
            "Contract_Type": "",
            "Product": "",
            "Premium": "",
        }
        income = {
            "Living_Benefit": "",
            "Withdrawal_Type": "",
            "Income_Start_Age": "",
            "Initial_Income_Rate": "",
            "Income_Percentage_Increase": "",
            "Guaranteed_Growth_Rate": "",
        }
        extra_income: Dict[str, str] = {}

        living_benefit_open = False
        for i, line in enumerate(lines):
            parsed = cls._parse_key_value_line(line)
            if not parsed:
                parsed = cls._parse_inline_key_value(line)
            if not parsed:
                if living_benefit_open and ":" not in line and not line.lower().startswith("withdrawal"):
                    income["Living_Benefit"] = f"{income['Living_Benefit']} {line}".strip()
                continue

            raw_key, raw_value = parsed
            key_norm = normalize_key(raw_key)
            value = raw_value.strip()
            if not value and i + 1 < len(lines):
                next_line = lines[i + 1]
                if next_line and ":" not in next_line:
                    value = next_line

            if key_norm in cls.PROFILE_FIELDS:
                profile[cls.PROFILE_FIELDS[key_norm]] = value
                living_benefit_open = False
                continue

            if key_norm in cls.INCOME_FIELDS:
                target_key = cls.INCOME_FIELDS[key_norm]
                if target_key == "Income_Start_Age":
                    age_match = re.search(r"\b(\d{1,3})\b", value)
                    if age_match:
                        value = age_match.group(1)
                income[target_key] = value
                living_benefit_open = target_key == "Living_Benefit"
                continue

            if value:
                key_words = ("income", "withdrawal", "benefit", "rate", "activation", "fee")
                if any(word in key_norm for word in key_words):
                    extra_income[sanitize_column_name(raw_key)] = value
            living_benefit_open = False

            # Handle line splits for value on next line (e.g. Age At Activation / 67)
            if not value and i + 1 < len(lines):
                next_line = lines[i + 1]
                if next_line and ":" not in next_line:
                    if key_norm in cls.INCOME_FIELDS:
                        income[cls.INCOME_FIELDS[key_norm]] = next_line
                    elif key_norm in cls.PROFILE_FIELDS:
                        profile[cls.PROFILE_FIELDS[key_norm]] = next_line

        for k, v in extra_income.items():
            if k not in income:
                income[k] = v
        return profile, income, cls._parse_strategy(lines)

    @staticmethod
    def _parse_key_value_line(line: str) -> Optional[Tuple[str, str]]:
        if ":" not in line:
            return None
        key, value = line.split(":", 1)
        return key.strip(), value.strip()

    @staticmethod
    def _parse_inline_key_value(line: str) -> Optional[Tuple[str, str]]:
        inline_patterns = [
            r"^(Owner)\s+(.+)$",
            r"^(Issue Age)\s+(.+)$",
            r"^(Contract Type)\s+(.+)$",
            r"^(Product(?: Name)?)\s+(.+)$",
            r"^(Initial Premium)\s+(.+)$",
            r"^(Solicitation State)\s+(.+)$",
            r"^(Living Benefit)\s+(.+)$",
            r"^(Withdrawal Type)\s+(.+)$",
            r"^(Withdrawal Rate)\s+(.+)$",
            r"^(Age at activation(?: date)?)\s*(.*)$",
            r"^(Age at lifetime income activation)\s*(.*)$",
            r"^(Age at 1st)\s*(.*)$",
            r"^(Age at 1st withdrawal)\s*(.*)$",
            r"^(Age at first withdrawal)\s*(.*)$",
            r"^(Income Start Age)\s*(.*)$",
        ]
        for pat in inline_patterns:
            match = re.match(pat, line, flags=re.IGNORECASE)
            if match:
                value = match.group(2).strip() if match.lastindex and match.lastindex >= 2 else ""
                return match.group(1).strip(), value
        return None

    @staticmethod
    def _parse_strategy(lines: List[str]) -> Dict[str, List[str]]:
        strategy = {
            "Strategy": [],
            "Rate": [],
            "Allocation": [],
            "Participation_Rate": [],
        }
        header_idx = -1
        for idx, line in enumerate(lines):
            lowered = line.lower()
            if "strategy" in lowered and "allocation" in lowered and "rate" in lowered:
                header_idx = idx
                break
        first_table_idx = len(lines)
        for idx, line in enumerate(lines):
            if line.lower().startswith("hypothetical values"):
                first_table_idx = idx
                break

        parse_start = header_idx + 1 if header_idx >= 0 else 0
        strategy_lines = lines[parse_start:first_table_idx]
        cleaned_lines: List[str] = []
        for line in strategy_lines:
            lowered = line.lower()
            if ":" in line and not lowered.startswith("strategy"):
                continue
            if (
                lowered.startswith("owner")
                or lowered.startswith("issue age")
                or lowered.startswith("contract type")
                or lowered.startswith("product")
                or lowered.startswith("initial premium")
                or lowered.startswith("living benefit")
                or lowered.startswith("withdrawal type")
                or lowered.startswith("withdrawal rate")
                or lowered.startswith("age at")
                or lowered.startswith("profile")
                or lowered.startswith("income details")
                or lowered.startswith("interest crediting strategy")
                or lowered.startswith("step-up")
            ):
                continue
            cleaned_lines.append(line)

        merged_lines: List[str] = []
        i = 0
        while i < len(cleaned_lines):
            current = cleaned_lines[i]
            if i + 2 < len(cleaned_lines):
                mid = cleaned_lines[i + 1]
                end = cleaned_lines[i + 2]
                if current.endswith("-") and re.match(r"^(?:\d|N/A)", mid, flags=re.IGNORECASE) and ":" not in end:
                    merged_lines.append(f"{current}{end} {mid}")
                    i += 3
                    continue
            if merged_lines and re.match(r"^(?:\d|N/A)", current, flags=re.IGNORECASE):
                merged_lines[-1] = f"{merged_lines[-1]} {current}"
            else:
                merged_lines.append(current)
            i += 1

        for line in merged_lines:
            metrics = re.findall(r"(?:\d+(?:\.\d+)?%|N/A)", line, flags=re.IGNORECASE)
            if len(metrics) < 2:
                continue
            first_metric = re.search(r"(?:\d+(?:\.\d+)?%|N/A)", line, flags=re.IGNORECASE)
            if not first_metric:
                continue

            name = line[: first_metric.start()].strip(" -")
            if not name or normalize_key(name) in {"rate", "allocation", "caprateallocation"}:
                continue
            if "fee" in name.lower():
                continue

            participation = ""
            rate = ""
            allocation = ""
            if len(metrics) >= 3:
                participation, rate, allocation = metrics[-3], metrics[-2], metrics[-1]
            elif len(metrics) == 2:
                rate, allocation = metrics[-2], metrics[-1]

            strategy["Strategy"].append(name)
            strategy["Rate"].append(rate)
            strategy["Allocation"].append(allocation)
            strategy["Participation_Rate"].append(participation)

        # Fallback: some PDFs split strategy names and numeric rows separately.
        # Example:
        #   S&P Annual PTP Performance-
        #   Triggered
        #   PIMCO Annual PTP Participation
        #   Rate
        #   100% 2.25% 50%
        #   25% N/A 50%
        if not strategy["Strategy"]:
            metric_lines = [
                ln
                for ln in cleaned_lines
                if re.match(r"^(?:\d+(?:\.\d+)?%|N/A)", ln, flags=re.IGNORECASE)
                and len(re.findall(r"(?:\d+(?:\.\d+)?%|N/A)", ln, flags=re.IGNORECASE)) >= 2
            ]
            if metric_lines:
                metric_start_idx = min(cleaned_lines.index(ln) for ln in metric_lines)
                name_source = cleaned_lines[:metric_start_idx]
            else:
                name_source = cleaned_lines

            def is_header_stub(value: str) -> bool:
                key = normalize_key(value)
                return key in {
                    "strategyparticipation",
                    "caprateallocation",
                    "allocation",
                    "allocationpercent",
                    "participation",
                    "caprate",
                    "",
                } or value.strip() == "%"

            candidate_names: List[str] = []
            for ln in name_source:
                if is_header_stub(ln):
                    continue
                if re.match(r"^(?:\d+(?:\.\d+)?%|N/A)", ln, flags=re.IGNORECASE):
                    continue
                if normalize_key(ln) in {"rate"}:
                    # keep standalone "Rate" only as continuation for prior line
                    if candidate_names:
                        candidate_names[-1] = f"{candidate_names[-1]} Rate".strip()
                    continue
                if normalize_key(ln) in {"triggered"} and candidate_names:
                    if candidate_names[-1].endswith("-"):
                        candidate_names[-1] = f"{candidate_names[-1][:-1]}-Triggered".strip()
                    else:
                        candidate_names[-1] = f"{candidate_names[-1]} Triggered".strip()
                    continue
                if candidate_names and candidate_names[-1].endswith("-"):
                    candidate_names[-1] = f"{candidate_names[-1][:-1]}{ln}".strip()
                else:
                    candidate_names.append(ln.strip())

            if metric_lines and candidate_names:
                # Pair last N candidate names with N numeric lines.
                names = candidate_names[-len(metric_lines) :]
                for name, metric_line in zip(names, metric_lines):
                    metrics = re.findall(r"(?:\d+(?:\.\d+)?%|N/A)", metric_line, flags=re.IGNORECASE)
                    if len(metrics) >= 3:
                        participation, rate, allocation = metrics[-3], metrics[-2], metrics[-1]
                    elif len(metrics) == 2:
                        participation, rate, allocation = "", metrics[-2], metrics[-1]
                    else:
                        continue
                    strategy["Strategy"].append(name)
                    strategy["Rate"].append(rate)
                    strategy["Allocation"].append(allocation)
                    strategy["Participation_Rate"].append(participation)
        return strategy


class ScenarioParser:
    REQUIRED_CANONICAL_COLUMNS = [
        "Year",
        "Age",
        "Interest_Earned",
        "Contract_Anniversary_Value",
        "Minimum_Accumulation_Value",
        "Cash_Surrender_Value",
        "Income_Base",
        "Annual_Income_for_Life",
        "Guaranteed_Lifetime_Income_Amount",
        "Withdrawal_Amount",
        "Income_Credit",
        "Cumulative_Withdrawal",
        "Death_Benefit",
    ]

    def parse_all(self, pages: List[List[str]]) -> Dict[str, Dict[str, List[str]]]:
        scenarios = {
            "zero_growth": {},
            "specific": {},
            "constant_growth": {},
            "favorable": {},
            "unfavorable": {},
        }

        zero_page = self._find_main_scenario_page(pages, "zero")
        specific_page = self._find_main_scenario_page(pages, "specific")
        constant_page = self._find_main_scenario_page(pages, "constant")

        if zero_page is not None:
            scenarios["zero_growth"] = self._parse_table_from_page(pages[zero_page], force_31_rows=True)
        if specific_page is not None:
            scenarios["specific"] = self._parse_table_from_page(pages[specific_page], force_31_rows=True)
        if constant_page is not None:
            scenarios["constant_growth"] = self._parse_table_from_page(pages[constant_page], force_31_rows=True)

        fav = self._find_named_specific(pages, "favorable specific period illustration")
        unfav = self._find_named_specific(pages, "unfavorable specific period illustration")
        if fav:
            scenarios["favorable"] = fav
        if unfav:
            scenarios["unfavorable"] = unfav

        return scenarios

    def _find_main_scenario_page(self, pages: List[List[str]], kind: str) -> Optional[int]:
        best: Optional[int] = None
        for i, lines in enumerate(pages):
            full = " ".join(lines).lower()
            row_count = self._count_valid_rows(lines)
            if row_count < 25:
                continue

            if kind == "zero":
                cond = (
                    "hypothetical values" in full
                    and ("minimum rates" in full or "0% credited interest" in full or "0% index interest" in full)
                    and "favorable specific period illustration" not in full
                    and "unfavorable specific period illustration" not in full
                )
            elif kind == "specific":
                cond = (
                    "hypothetical values" in full
                    and ("current rates" in full or "current/specific period rates" in full or "specific period" in full)
                    and "minimum rates" not in full
                    and "assumed rate" not in full
                    and "assumed index interest rate" not in full
                    and "favorable specific period illustration" not in full
                    and "unfavorable specific period illustration" not in full
                )
            else:  # constant
                cond = (
                    "hypothetical values" in full
                    and ("assumed rate" in full or "assumed index interest rate" in full)
                    and "favorable specific period illustration" not in full
                    and "unfavorable specific period illustration" not in full
                )

            if cond:
                best = i
                break
        return best

    def _find_named_specific(self, pages: List[List[str]], title: str) -> Dict[str, List[str]]:
        for lines in pages:
            lowered_lines = [line.lower() for line in lines]
            heading_positions = [idx for idx, line in enumerate(lowered_lines) if title in line]
            if not heading_positions:
                continue

            for pos in heading_positions:
                end = len(lines)
                for j in range(pos + 1, len(lines)):
                    if (
                        "favorable specific period illustration" in lowered_lines[j]
                        or "unfavorable specific period illustration" in lowered_lines[j]
                    ):
                        end = j
                        break
                block = lines[pos:end]
                if self._count_valid_rows(block) >= 8:
                    return self._parse_table_from_page(block, force_31_rows=False)
        return {}

    @staticmethod
    def _count_valid_rows(lines: List[str]) -> int:
        return sum(1 for line in lines if ROW_RE.match(line))

    def _parse_table_from_page(self, lines: List[str], force_31_rows: bool) -> Dict[str, List[str]]:
        row_indices = [idx for idx, line in enumerate(lines) if ROW_RE.match(line)]
        if not row_indices:
            return {}
        first_row_idx = row_indices[0]
        header_lines = lines[max(0, first_row_idx - 30) : first_row_idx]
        row_lines = [lines[idx] for idx in row_indices]

        raw_columns = self._extract_columns(header_lines)
        if not raw_columns:
            max_width = max((len(self._extract_row_tokens(r)[2]) for r in row_lines), default=0)
            raw_columns = ["Year", "Age"] + [f"Column_{i+1}" for i in range(max_width)]

        canonical_columns = dedupe_columns([canonical_column_name(c) for c in raw_columns])
        parsed_rows = [self._row_to_dict(line, canonical_columns) for line in row_lines]
        parsed_rows = [row for row in parsed_rows if row]

        # Keep earliest row per year token.
        ordered_rows: List[Dict[str, str]] = []
        seen_years = set()
        for row in parsed_rows:
            year = row.get("Year", "")
            if not year or year in seen_years:
                continue
            seen_years.add(year)
            ordered_rows.append(row)

        if force_31_rows:
            ordered_rows = self._force_31_year_rows(ordered_rows, canonical_columns)

        scenario_dict = {col: [row.get(col, "") for row in ordered_rows] for col in canonical_columns}
        scenario_dict = self._ensure_required_columns(scenario_dict)
        return scenario_dict

    def _extract_columns(self, header_lines: List[str]) -> List[str]:
        if not header_lines:
            return []
        header_text = normalize_text(" ".join(header_lines))
        lowered = header_text.lower()
        lowered = lowered.replace("contract's", "contract s")
        lowered = lowered.replace("hypothetical ", "")
        lowered = re.sub(r"\s+", " ", lowered)

        columns = ["Year", "Age"]

        start = lowered.find("year age")
        if start < 0:
            start = lowered.find("year")
        if start < 0:
            return []

        fixed_anchor_patterns = [
            r"credited interest rate",
            r"interest earned",
            r"withdrawal amount",
            r"contract s anniversary value",
            r"cash surrender value",
            r"income base",
            r"death benefit",
        ]
        anchor_positions = []
        for pattern in fixed_anchor_patterns:
            match = re.search(pattern, lowered[start:])
            if match:
                anchor_positions.append(start + match.start())
        end = min(anchor_positions) if anchor_positions else len(lowered)
        index_segment = lowered[start:end]

        index_cols = []
        acc: List[str] = []
        for word in index_segment.split():
            acc.append(word)
            if word == "change":
                col = " ".join(acc).strip()
                col = col.replace("year age", "").strip()
                if col and col not in {"year", "age"}:
                    index_cols.append(col)
                acc = []

        for col in index_cols:
            pretty = re.sub(r"\s+", " ", col).strip().title()
            columns.append(pretty)

        fixed_patterns = [
            (r"credited interest rate", "Credited Interest Rate"),
            (r"interest earned", "Interest Earned"),
            (r"withdrawal amount", "Withdrawal Amount"),
            (r"guaranteed return of premium grop", "Guaranteed Return of Premium GROP"),
            (r"overlay value", "Overlay Value"),
            (r"contract s anniversary value|contract anniversary value", "Contract Anniversary Value"),
            (r"minimum accumulation value", "Minimum Accumulation Value"),
            (r"cash surrender value", "Cash Surrender Value"),
            (r"income base", "Income Base"),
            (r"annual income for life", "Annual Income for Life"),
            (r"guaranteed lifetime income amount", "Guaranteed Lifetime Income Amount"),
            (r"income credit percentage|income credit", "Income Credit"),
            (r"cumulative withdrawal", "Cumulative Withdrawal"),
            (r"death benefit", "Death Benefit"),
        ]
        matches: List[Tuple[int, int, str]] = []
        for pattern, canonical in fixed_patterns:
            for match in re.finditer(pattern, lowered):
                matches.append((match.start(), match.end(), canonical))
        matches.sort(key=lambda item: (item[0], -(item[1] - item[0])))

        chosen: List[Tuple[int, int, str]] = []
        last_end = -1
        for start_idx, end_idx, canonical in matches:
            if start_idx < start:
                continue
            if start_idx < last_end:
                continue
            chosen.append((start_idx, end_idx, canonical))
            last_end = end_idx
        columns.extend([item[2] for item in chosen])
        return columns

    def _extract_row_tokens(self, line: str) -> Tuple[str, str, List[str]]:
        row = normalize_text(line)
        tokens = row.split()
        if not tokens:
            return "", "", []

        if len(tokens) >= 3 and tokens[0].lower() == "at" and tokens[1].lower() == "issue":
            year = "At Issue"
            age_token = tokens[2]
            remaining = tokens[3:]
        else:
            year = tokens[0]
            age_token = tokens[1] if len(tokens) > 1 else ""
            remaining = tokens[2:] if len(tokens) > 2 else []

        age = age_token
        if age_token.endswith("-"):
            age = age_token[:-1]
            remaining = ["-"] + remaining

        remaining = [tok for tok in remaining if tok]
        return year, age, remaining

    def _row_to_dict(self, line: str, columns: List[str]) -> Dict[str, str]:
        year, age, values = self._extract_row_tokens(line)
        if not year:
            return {}

        expected = max(0, len(columns) - 2)
        if len(values) > expected:
            cleaned = [
                v
                for v in values
                if re.search(r"[0-9$%]|^-$|^N/A$", v, flags=re.IGNORECASE)
            ]
            values = cleaned if len(cleaned) >= expected else values
        if len(values) > expected:
            values = values[:expected]
        if len(values) < expected:
            values = values + [""] * (expected - len(values))

        row_dict = {"Year": year, "Age": age}
        for idx, col in enumerate(columns[2:]):
            row_dict[col] = values[idx] if idx < len(values) else ""
        return row_dict

    def _force_31_year_rows(
        self, rows: List[Dict[str, str]], columns: List[str]
    ) -> List[Dict[str, str]]:
        row_map = {row.get("Year", ""): row for row in rows}
        ordered_keys = ["At Issue"] + [str(i) for i in range(1, 31)]
        output: List[Dict[str, str]] = []
        for year_key in ordered_keys:
            if year_key in row_map:
                output.append(row_map[year_key])
            else:
                blank = {col: "" for col in columns}
                blank["Year"] = year_key
                output.append(blank)
        return output

    def _ensure_required_columns(
        self, data: Dict[str, List[str]]
    ) -> Dict[str, List[str]]:
        n_rows = 0
        for v in data.values():
            n_rows = max(n_rows, len(v))
        for required in self.REQUIRED_CANONICAL_COLUMNS:
            if required not in data:
                data[required] = [""] * n_rows
        return data


class AnnuityExtractorPipeline:
    def __init__(
        self,
        pdf_dir: Path,
        output_json: Path,
        drop_file: Path,
        extractor_backend: str = "auto",
    ) -> None:
        self.pdf_dir = pdf_dir
        self.output_json = output_json
        self.drop_file = drop_file
        self.text_extractor = build_text_extractor(backend=extractor_backend)
        self.scenario_parser = ScenarioParser()

    def run(self) -> Tuple[Dict[str, dict], List[str]]:
        product_structure: Dict[str, dict] = {}
        dropped: List[str] = []

        pdf_files = sorted([p for p in self.pdf_dir.glob("*.pdf") if p.is_file()])
        for pdf_path in pdf_files:
            try:
                pages = self.text_extractor.extract_pages(pdf_path)
            except Exception:
                dropped.append(pdf_path.name)
                continue

            scenarios = self.scenario_parser.parse_all(pages)
            first_scenario_lines = self._pick_first_scenario_lines(pages)
            profile, income_details, strategy = SectionParser.parse_sections(first_scenario_lines)

            product_structure[pdf_path.name] = {
                "Profile": profile,
                "income_details": income_details,
                "interest_crediting_strategy": strategy,
                "scenario": scenarios,
            }

            if not self._is_successful_extraction(scenarios):
                dropped.append(pdf_path.name)

        self.output_json.write_text(json.dumps(product_structure, indent=2), encoding="utf-8")
        drop_text = "\n".join(dropped)
        if drop_text:
            drop_text += "\n"
        self.drop_file.write_text(drop_text, encoding="utf-8")
        return product_structure, dropped

    @staticmethod
    def _pick_first_scenario_lines(pages: List[List[str]]) -> List[str]:
        # Prefer a scenario page that also contains the profile/income header.
        for lines in pages:
            full = " ".join(lines).lower()
            if (
                "hypothetical values" in full
                and ScenarioParser._count_valid_rows(lines) >= 10
                and ("profile income details interest crediting strategy" in full or ("owner" in full and "issue age" in full))
            ):
                return lines
        for lines in pages:
            full = " ".join(lines).lower()
            if "hypothetical values" in full and ScenarioParser._count_valid_rows(lines) >= 10:
                return lines
        return pages[0] if pages else []

    @staticmethod
    def _is_successful_extraction(scenarios: Dict[str, Dict[str, List[str]]]) -> bool:
        for key in ("zero_growth", "specific", "constant_growth"):
            rows = scenarios.get(key, {}).get("Year", [])
            if len(rows) < 31:
                return False
        return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract annuity data from PDFs.")
    parser.add_argument(
        "--pdf-dir",
        type=Path,
        default=Path("pdf"),
        help="Directory containing PDF files.",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=Path("product_structure.json"),
        help="Output JSON path.",
    )
    parser.add_argument(
        "--drop-file",
        type=Path,
        default=Path("drop_pdf"),
        help="Output text file for unmatched PDFs.",
    )
    parser.add_argument(
        "--pdf-extractor",
        choices=["auto", "pypdf", "pypdf2", "pdfkit"],
        default="auto",
        help="PDF text extraction backend. 'auto' prefers pypdf/PyPDF2 and falls back to macOS pdfkit.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        pipeline = AnnuityExtractorPipeline(
            pdf_dir=args.pdf_dir,
            output_json=args.output_json,
            drop_file=args.drop_file,
            extractor_backend=args.pdf_extractor,
        )
    except RuntimeError as exc:
        print(f"Error initializing PDF extractor: {exc}", file=sys.stderr)
        print(
            "Tip: On Windows/Linux install pypdf with `pip install pypdf` and use `--pdf-extractor auto`.",
            file=sys.stderr,
        )
        return 2
    _, dropped = pipeline.run()
    print(f"Completed. drop_pdf count: {len(dropped)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
