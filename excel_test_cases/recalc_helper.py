"""
recalc_helper.py  —  uses xlwings to open, switch scenario, recalculate and save
each workbook in the excel_test_cases folder.

Usage:
    python recalc_helper.py ./excel_test_cases
"""
import sys
import time
from pathlib import Path

try:
    import xlwings as xw
except ImportError:
    print("xlwings not installed.  Run: pip install xlwings")
    sys.exit(1)

SCENARIO_NAMED_RANGE = "Summary"
SCENARIOS = {
    "specific":        "Specific",
    "zero_growth":     "Zero Growth",
    "constant_growth": "Constant Growth",
}

def _configure_app(app):
    # Reduce UI prompts during batch automation.
    try:
        app.display_alerts = False
    except Exception:
        pass
    try:
        app.screen_updating = False
    except Exception:
        pass
    try:
        app.calculation = "automatic"
    except Exception:
        pass
    try:
        app.api.enable_events = False
    except Exception:
        pass
    try:
        app.api.ask_to_update_links = False
    except Exception:
        pass

def recalc_workbook(app, xl_path: Path):
    print(f"[recalc] Opening {xl_path.name} ...")
    wb = None
    try:
        # update_links=False avoids repeated "update links/grant access" prompts
        # when files contain references.
        try:
            wb = app.books.open(
                str(xl_path.resolve()),
                update_links=False,
                read_only=False,
                notify=False,
                add_to_mru=False,
            )
        except TypeError:
            # Older xlwings builds may not support all kwargs.
            wb = app.books.open(str(xl_path.resolve()))

        for scenario_key, excel_val in SCENARIOS.items():
            print(f"  scenario={scenario_key} ({excel_val})")
            # Write Summary named range
            rng = wb.names[SCENARIO_NAMED_RANGE].refers_to_range
            rng.value = excel_val
            # Force recalculate
            wb.app.calculate()
            time.sleep(3)   # give Excel time to finish

            # Read & cache outputs here if needed (or just save and let Phase 2 handle it)

        # Leave as Specific before final save
        rng = wb.names[SCENARIO_NAMED_RANGE].refers_to_range
        rng.value = "Specific"
        wb.app.calculate()
        time.sleep(2)
        wb.save()
        print(f"  Saved {xl_path.name}")
    finally:
        if wb is not None:
            wb.close()

if __name__ == "__main__":
    folder = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(".")
    xlsx_files = sorted(folder.glob("*.xlsx"))
    if not xlsx_files:
        print(f"No .xlsx files found in {folder}")
        sys.exit(1)

    app = xw.App(visible=False, add_book=False)
    _configure_app(app)
    try:
        for f in xlsx_files:
            recalc_workbook(app, f)
    finally:
        app.quit()
    print("Done.")
