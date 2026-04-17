# Product Illustration Automation

This project automates the validation workflow for annuity illustration PDF test cases.

At a high level, it:
- extracts structured input data from PDF test cases
- populates Excel illustration workbooks
- recalculates workbooks in Excel
- gathers Report-tab outputs
- compares tool outputs against PDF-derived values

## Main Scripts

- [annuity_automation.py](annuity_automation.py)
  Integrated end-to-end runner for extraction, workbook population, recalculation, output gathering, and comparison.
- [extract_annuity_data.py](extract_annuity_data.py)
  Standalone PDF extraction script that generates `product_structure.json` and `drop_report.csv`.

## Demo Scripts

The demo scripts split the workflow into three steps:

- [demo_1_extract.py](demo%20scripts/demo_1_extract.py)
  Extract PDFs into `product_structure.json`
- [demo_2_populate_recalc.py](demo%20scripts/demo_2_populate_recalc.py)
  Populate workbooks and recalculate them in Excel
- [demo_3_gather_compare.py](demo%20scripts/demo_3_gather_compare.py)
  Gather tool outputs and compare them with extracted PDF values

Demo outputs are written under:
- `demo scripts/demo/product_structure.json`
- `demo scripts/demo/drop_report.csv`
- `demo scripts/demo/excel_test_cases/`
- `demo scripts/demo/tool_calc_output.json`
- `demo scripts/demo/check_report.csv`

## Integrated Run

The integrated script can run extraction, workbook population, recalc, gather, and compare in one submission.

Typical flow:

1. Extract PDFs into a run-specific `product_structure.json`
2. Create one workbook per test case in `excel_test_cases`
3. Recalculate workbooks in Excel
4. Capture scenario-specific Report outputs during recalc
5. Build `tool_calc_output.json`
6. Build `check_report.csv` and `error_report.csv`

### Run Output Structure

By default, runs are written under:

`results/YYYY-MM-DD/valuation_<timestamp>_<id>/`

Typical files:
- `product_structure.json`
- `drop_report.csv`
- `run_config.json`
- `run.log`
- `tool_calc_output.json`
- `check_report.csv`
- `error_report.csv`
- `excel_test_cases/`

Inside `excel_test_cases/`:
- populated `.xlsx` workbooks
- `recalc_helper.py`
- `*.scenario_output.json` captured during Excel recalculation

## Extraction Rules

Extraction produces:
- `product_structure.json` for PDFs kept in scope
- `drop_report.csv` for PDFs intentionally dropped

Current drop behavior:
- PDFs are dropped if text extraction fails
- PDFs are dropped if product name indicates unsupported products such as `Polaris` or `Polaris Platinum`
- If a file is dropped, it is not written into `product_structure.json`

Incomplete scenario extraction alone does not currently force a drop.

## Scenario Handling

Scenario-specific Report outputs are captured during the Excel recalculation loop for:
- `specific`
- `zero_growth`
- `constant_growth`

Phase 2 uses the captured `*.scenario_output.json` sidecar files when available. This avoids rereading the same saved workbook state three times.

## Workbook Naming

Generated workbooks are named without the `.pdf` suffix.

Example:
- input PDF: `AG Choice Index 10_301.pdf`
- workbook: `AG Choice Index 10_301.xlsx`

## Environment Notes

### Windows

Recommended:
- Microsoft Excel desktop installed
- Python available in the interpreter you will run
- required packages installed in that interpreter

Typical packages:

```powershell
python -m pip install openpyxl xlwings pypdf
```

### macOS

Excel automation is also supported on macOS. The recalc helper uses platform-specific staging logic to improve reliability.

## Integrated Script Usage

Run extraction + automation together:

```bash
python annuity_automation.py --template "/path/to/Index Annuity Hypo Illustrations Tool_v1.47.xlsx" --pdf-dir "/path/to/pdf" --phases 1 2 3
```

Run only specific test cases:

```bash
python annuity_automation.py --template "/path/to/Index Annuity Hypo Illustrations Tool_v1.47.xlsx" --pdf-dir "/path/to/pdf" --phases 1 2 3 --test-cases "AG Choice Index 10_301.pdf"
```

Run only phases 2 and 3 against an existing run folder:

```bash
python annuity_automation.py --output-dir "/path/to/results/YYYY-MM-DD/valuation_xxx" --json "/path/to/results/YYYY-MM-DD/valuation_xxx/product_structure.json" --phases 2 3
```

## Demo Usage

### Demo 1

```powershell
& "C:\Path\To\python.exe" ".\demo scripts\demo_1_extract.py" --pdf-dir "C:\path\to\pdf" --pdf-extractor auto
```

### Demo 2

```powershell
& "C:\Path\To\python.exe" ".\demo scripts\demo_2_populate_recalc.py" --template "C:\path\to\Index Annuity Hypo Illustrations Tool_v1.47.xlsx"
```

### Demo 3

```powershell
& "C:\Path\To\python.exe" ".\demo scripts\demo_3_gather_compare.py"
```

## Known Limitations

- PDF layouts vary, so some profile or scenario fields may still require parser enhancements
- Tool/PDF mismatches do not automatically mean the automation is wrong; differences may come from extraction, workbook population, tool behavior, or test-case assumptions
- Business review of populated workbooks is still important for issue triage

## Recommended Review Process

For mismatched cases:

1. Open the populated workbook
2. Review whether inputs were populated correctly
3. Review Report-tab outputs
4. Log issues with:
   - test case name
   - expected behavior
   - actual behavior
   - whether it looks like an extraction, automation, or tool issue

This issue log can then be used to prioritize code changes.
