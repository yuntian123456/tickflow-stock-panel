"""tdxgo 插件的 Python↔Go subprocess 桥接与可用性检测。

模式对齐 stocksdk 的 bridge.py: 把单个 JSON 请求写进 Go 二进制的 stdin,
从 stdout 读单个 JSON 响应。Go 二进制由 go build 编译到本插件目录下的 bin/。
"""

from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path

_PLUGIN_DIR = Path(__file__).resolve().parent

# injoyai/tdx 底层(injoyai/ios)会把连接日志打到 stdout, 先剥离 ANSI 再取 JSON。
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


class TdxGoBridgeError(RuntimeError):
    """Go 桥接调用失败(二进制缺失/超时/非零退出/非法 JSON/!ok)。"""


def _extract_json(text: str) -> str:
    """从混合了日志的 stdout 中按花括号配平提取唯一的 JSON 对象子串。"""
    idx = text.find("{")
    if idx < 0:
        return ""
    depth = 0
    in_str = False
    esc = False
    for i in range(idx, len(text)):
        ch = text[i]
        if esc:
            esc = False
            continue
        if ch == "\\":
            esc = True
            continue
        if ch == '"':
            in_str = not in_str
            continue
        if in_str:
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[idx : i + 1]
    return ""


def _binary() -> Path:
    """定位 Go 桥接二进制(Windows 下为 .exe, 其余平台无扩展名)。"""
    name = "tdxgo.exe" if os.name == "nt" else "tdxgo"
    env_bin = os.environ.get("TDXGO_BIN")
    if env_bin:
        p = Path(env_bin)
        if p.is_file():
            return p
    p = _PLUGIN_DIR / "bin" / name
    if p.is_file():
        return p
    return p


def _timeout(job: dict) -> float:
    """按 op 区分超时: 全量日K/分钟/标的可能较慢, realtime/单标的给短些。"""
    op = job.get("op")
    if op in ("daily", "minute", "adj_factor", "instruments"):
        return 90.0
    return 30.0


def run_job(job: dict) -> object:
    """执行一次桥接调用, 返回响应中的 data; 失败抛 TdxGoBridgeError。"""
    binary = _binary()
    if not binary.exists():
        raise TdxGoBridgeError(
            "未找到 tdxgo 桥接二进制, 请先编译: cd backend/app/plugins/tdxgo && go build -o bin/tdxgo.exe ."
        )
    payload = json.dumps(job).encode("utf-8")
    try:
        proc = subprocess.run(
            [str(binary)],
            input=payload,
            capture_output=True,
            timeout=_timeout(job),
            cwd=str(_PLUGIN_DIR),
        )
    except subprocess.TimeoutExpired as e:
        raise TdxGoBridgeError(f"tdxgo 执行超时: op={job.get('op')}") from e
    except OSError as e:
        raise TdxGoBridgeError(f"tdxgo 启动失败: {e}") from e

    if proc.returncode != 0:
        raise TdxGoBridgeError(f"tdxgo 非零退出({proc.returncode}): {proc.stderr.decode('utf-8', 'ignore')}")
    text = _ANSI_RE.sub("", proc.stdout.decode("utf-8", "ignore"))
    json_text = _extract_json(text)
    try:
        resp = json.loads(json_text or "{}")
    except json.JSONDecodeError as e:
        raise TdxGoBridgeError(f"tdxgo 非法 JSON 响应: {e}") from e
    if not resp.get("ok"):
        raise TdxGoBridgeError(f"tdxgo 返回错误: {resp.get('error')}")
    return resp.get("data")


def availability() -> tuple[bool, str]:
    """返回 (是否可用, 原因)。不抛异常。检查二进制存在并 ping 探活。"""
    binary = _binary()
    if not binary.exists():
        return False, "未找到 tdxgo 二进制, 请执行: cd backend/app/plugins/tdxgo && go build -o bin/tdxgo.exe ."
    try:
        data = run_job({"op": "ping"})
        if isinstance(data, dict) and data.get("status") == "ok":
            return True, "ok"
        return False, f"ping 返回异常: {data}"
    except TdxGoBridgeError as e:
        return False, str(e)


# 供测试/外部复用的别名(对齐 stocksdk 导出)。
BridgeError = TdxGoBridgeError
run = run_job
