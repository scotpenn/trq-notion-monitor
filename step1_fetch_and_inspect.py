#!/usr/bin/env python3
"""
Step 1: 下载加拿大钢铁 TRQ 数据并摸清结构

做三件事：
  1. 下载 Q4 Non-FTA 和 FTA 两份 CSV
  2. 打印 HTTP Last-Modified 响应头（判断真实更新频率）
  3. 打印列名 + 前 20 行 + 筛出 EMT/RSC（HS 7306）相关行

运行：
    python3 step1_fetch_and_inspect.py

输出保存在 notes/ 和 data/ 子目录。只用 Python 标准库，无需 pip install。
"""

import os
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

# ---- 配置 ----
# 数据源 URL（Q4 = 当前季度 2026-03-26 至 2026-06-27）
SOURCES = {
    "NFTA-Q4": "https://www.eics-scei.gc.ca/report-rapport/TRQ_NFTA-Q4.csv",
    "FTA-Q4":  "https://www.eics-scei.gc.ca/report-rapport/TRQ_FTA-Q4.csv",
}

# EMT / RSC 关键词（用于从数据里快速筛出相关行）
EMT_KEYWORDS = ["pipe", "tube", "tubular", "7306", "conduit", "hollow"]

# 脚本所在目录
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
NOTES_DIR = BASE_DIR / "notes"
DATA_DIR.mkdir(exist_ok=True)
NOTES_DIR.mkdir(exist_ok=True)

# 今日日期，用于文件命名
TODAY = datetime.now().strftime("%Y-%m-%d")


def download_csv(label: str, url: str) -> tuple[str, dict]:
    """下载一个 CSV 文件，返回 (文件内容字符串, HTTP 响应头字典)"""
    print(f"[下载] {label}: {url}")
    req = urllib.request.Request(url, headers={"User-Agent": "TRQ-Monitor/0.1"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        headers = dict(resp.headers)
        raw = resp.read()

    # 保存原始 CSV（按日期快照）
    snapshot_path = DATA_DIR / f"{label}_{TODAY}.csv"
    snapshot_path.write_bytes(raw)
    print(f"       已保存到 {snapshot_path.name}（{len(raw):,} bytes）")

    # 解码为字符串（加拿大政府文件通常是 UTF-8 或 UTF-8-BOM）
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            text = raw.decode(enc)
            return text, headers
        except UnicodeDecodeError:
            continue
    raise RuntimeError(f"无法解码 {label} CSV，尝试过 utf-8 / cp1252 / latin-1")


def inspect_csv(label: str, text: str, headers: dict) -> list[str]:
    """分析 CSV 内容，返回要写入报告的文本行列表"""
    lines = text.splitlines()
    out = []
    out.append("=" * 70)
    out.append(f"数据源：{label}")
    out.append("=" * 70)

    # HTTP 头信息（判断更新频率的关键）
    out.append(f"HTTP Last-Modified : {headers.get('Last-Modified', '(无)')}")
    out.append(f"HTTP Content-Length: {headers.get('Content-Length', '(无)')}")
    out.append(f"HTTP Content-Type  : {headers.get('Content-Type', '(无)')}")
    out.append("")

    # 行数 / 列数统计
    out.append(f"总行数：{len(lines)}")
    if lines:
        header_line = lines[0]
        col_count = header_line.count(",") + 1
        out.append(f"列数（根据表头逗号）：{col_count}")
        out.append("")
        out.append("── 表头（第 1 行）──")
        out.append(header_line)
        out.append("")

    # 前 20 行数据
    out.append("── 前 20 行数据 ──")
    for i, line in enumerate(lines[:20], start=1):
        out.append(f"{i:>3}: {line}")
    out.append("")

    # 筛出 EMT/RSC 相关行
    out.append(f"── 含 EMT 关键词的行 (关键词: {EMT_KEYWORDS}) ──")
    matches = []
    for i, line in enumerate(lines, start=1):
        low = line.lower()
        if any(kw in low for kw in EMT_KEYWORDS):
            matches.append(f"{i:>4}: {line}")
    if matches:
        for m in matches[:50]:
            out.append(m)
        if len(matches) > 50:
            out.append(f"... 还有 {len(matches) - 50} 行，完整见原始 CSV")
    else:
        out.append("(没命中关键词 — 可能 EMT 归在别的字段里，需要肉眼检查表头)")
    out.append("")
    return out


def main():
    print(f"\n日期：{TODAY}\n")
    all_output = [f"Canada Steel TRQ 数据摸底报告 — {TODAY}", ""]

    for label, url in SOURCES.items():
        try:
            text, headers = download_csv(label, url)
            all_output.extend(inspect_csv(label, text, headers))
        except Exception as e:
            msg = f"[错误] {label} 下载/解析失败：{e}"
            print(msg)
            all_output.append(msg)
            all_output.append("")

    # 写入报告
    report_path = NOTES_DIR / f"step1_output_{TODAY}.txt"
    report_path.write_text("\n".join(all_output), encoding="utf-8")
    print(f"\n✅ 报告已保存：{report_path}")
    print("\n把这个文件内容发给 Claude，就可以进入 Step 2（设计 Notion schema）。")


if __name__ == "__main__":
    main()
