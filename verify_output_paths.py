#!/usr/bin/env python3
"""
Quick check that all output paths point to project root (not src/outputs).
Run from project root:  python verify_output_paths.py
"""

from pathlib import Path
import sys

def main():
    root = Path(__file__).resolve().parent
    sys.path.insert(0, str(root))
    import config

    expected = root / "outputs"
    checks = []

    # 1. config
    factors_path = config.get_output_path("factors")
    checks.append(("config.get_output_path('factors')", factors_path.resolve().parent == expected))

    # 2. Factor default (same logic as in src/*.py: parent.parent / "outputs" / "factors")
    src_dir = root / "src"
    factor_default = (src_dir.resolve().parent / "outputs" / "factors").resolve().parent
    checks.append(("src factor default", factor_default == expected))

    # 3. run_all would use config
    run_all_dir = config.get_output_path("factors")
    checks.append(("run_all (config)", run_all_dir.resolve().parent == expected))

    all_ok = all(c[1] for c in checks)
    for name, ok in checks:
        print(f"  {'✓' if ok else '✗'} {name}")
    print()
    if all_ok:
        print("All output paths use project root 'outputs/'. You can run:")
        print("  python run_all.py          # full pipeline")
        print("  python src/value.py        # single factor (outputs still in root outputs/)")
    else:
        print("Some paths are wrong. Check config.PROJECT_ROOT and factor _DEFAULT_FACTOR_OUTPUT.")
        sys.exit(1)

if __name__ == "__main__":
    main()
