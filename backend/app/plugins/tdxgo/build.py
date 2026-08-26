"""编译 tdxgo 桥接二进制。

用法: python build.py [--output PATH]
默认输出到本插件目录 bin/tdxgo(.exe)。需 Go 1.25+。
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

_PLUGIN_DIR = Path(__file__).resolve().parent


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output", default=None, help="输出二进制路径")
    args = ap.parse_args()

    suffix = ".exe" if sys.platform == "win32" else ""
    out = Path(args.output) if args.output else (_PLUGIN_DIR / "bin" / f"tdxgo{suffix}")
    out.parent.mkdir(parents=True, exist_ok=True)

    print(f"[tdxgo] go mod tidy ...")
    if subprocess.run(["go", "mod", "tidy"], cwd=str(_PLUGIN_DIR)).returncode != 0:
        return 1
    print(f"[tdxgo] go build -o {out} ...")
    return subprocess.run(["go", "build", "-o", str(out), "."], cwd=str(_PLUGIN_DIR)).returncode


if __name__ == "__main__":
    raise SystemExit(main())
