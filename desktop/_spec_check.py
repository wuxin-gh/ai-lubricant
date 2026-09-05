"""Validate AiLubricant.spec: datas paths exist and hiddenimports resolve.

Dev-only helper, not shipped in the bundle. Run from the repo root:

    .venv/Scripts/python.exe desktop/_spec_check.py
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "server"))

spec_src = (ROOT / "desktop" / "AiLubricant.spec").read_text(encoding="utf-8")
head = spec_src[: spec_src.index("a = Analysis(")]
# spec 里 PROJECT_ROOT = Path(SPECPATH).parent，故这里传 desktop/ 目录。
ns: dict = {"SPECPATH": str(ROOT / "desktop")}
exec(compile(head, "spec-head", "exec"), ns)  # noqa: S102

missing_datas = [entry[0] for entry in ns["datas"] if not Path(entry[0]).exists()]
print(f"datas entries      : {len(ns['datas'])}")
print(f"missing datas      : {missing_datas or 'none'}")

hidden = ns["hiddenimports"]
print(f"hiddenimports total: {len(hidden)}")

bad: list[str] = []
for name in hidden:
    try:
        if importlib.util.find_spec(name) is None:
            bad.append(name)
    except Exception as exc:  # noqa: BLE001
        bad.append(f"{name} ({type(exc).__name__})")

print(f"unresolvable       : {len(bad)}")
for name in bad:
    print(f"  - {name}")

icon = ROOT / "user-frontend" / "electron" / "icon.png"
print(f"icon exists        : {icon.exists()}")

sys.exit(1 if (missing_datas or bad) else 0)
