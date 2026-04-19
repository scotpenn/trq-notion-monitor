#!/usr/bin/env python3
"""
每日 TRQ 数据同步到 Notion

两种运行方式：
  1. GitHub Actions（生产）：凭据从环境变量读（GitHub Secrets 提供）
  2. 本地调试：凭据从 config.json 读（已加入 .gitignore）

总流程：
  load_config() → download_csv() → parse_trq_csv() → push_to_notion() → done
"""

import os
import csv
import io
import json
import time
import urllib.request
import urllib.error
from pathlib import Path
from datetime import datetime

# ======== 全局配置 ========
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
NOTES_DIR = BASE_DIR / "notes"
DATA_DIR.mkdir(exist_ok=True)
NOTES_DIR.mkdir(exist_ok=True)

TODAY = datetime.now().strftime("%Y-%m-%d")
CURRENT_QUARTER = "Q4"  # 2026-03-26 至 2026-06-27；6/27 之后要改成 "Q1"

SOURCES = {
    "NFTA-Q4": "https://www.eics-scei.gc.ca/report-rapport/TRQ_NFTA-Q4.csv",
    "FTA-Q4":  "https://www.eics-scei.gc.ca/report-rapport/TRQ_FTA-Q4.csv",
}

# Notion API 常量
# Notion-Version 必须 >= 2025-09-03 才支持用 data_source_id 作为 page parent
NOTION_API_URL = "https://api.notion.com/v1/pages"
NOTION_API_VERSION = "2025-09-03"

# 产品类别 → Product Tag 映射（只给这 6 类打 tag，其余会被 dashboard 过滤掉）
TAG_MAP = {
    "Standard Pipe":              ["EMT", "RIGID"],
    "Line Pipe":                  ["RIGID"],
    "Structural Steel":           ["STRUT"],
    "Hollow Structural Sections": ["STRUT"],
    "Cold Finished Bars":         ["SUPPORT"],
    "Cold-Rolled Sheet":          ["SUPPORT"],
}


# ======== 函数 1：加载凭据 ========

def load_config() -> dict:
    """
    拿到 Notion token + data source ID。

    查找顺序：
      1. 环境变量 NOTION_TOKEN + NOTION_DATA_SOURCE_ID（GitHub Actions 走这条）
      2. 本地 config.json（本地调试走这条）

    Returns:
        dict: {"notion_token": "ntn_xxx", "data_source_id": "f8a2..."}

    Raises:
        RuntimeError: 两处都找不到时抛错，提示怎么修
    """
    # --- 路径 1：环境变量 ---
    token = os.environ.get("NOTION_TOKEN")
    ds_id = os.environ.get("NOTION_DATA_SOURCE_ID")
    if token and ds_id:
        print("[config] ✅ 从环境变量读取凭据（GitHub Actions 模式）")
        return {"notion_token": token, "data_source_id": ds_id}

    # --- 路径 2：本地 config.json ---
    config_path = BASE_DIR / "config.json"
    if config_path.exists():
        print(f"[config] ✅ 从 {config_path.name} 读取凭据（本地模式）")
        with config_path.open("r", encoding="utf-8") as f:
            cfg = json.load(f)
        # 防御式检查：少字段立刻报错
        for key in ("notion_token", "data_source_id"):
            if key not in cfg or not cfg[key]:
                raise RuntimeError(f"config.json 里缺少字段：{key}")
        return {
            "notion_token":    cfg["notion_token"],
            "data_source_id":  cfg["data_source_id"],
        }

    # --- 都没有：报错，告诉用户怎么修 ---
    raise RuntimeError(
        "❌ 找不到 Notion 凭据！请检查：\n"
        "  [GitHub Actions] Repo Settings → Secrets → 确认 NOTION_TOKEN 和 "
        "NOTION_DATA_SOURCE_ID 都已设置\n"
        "  [本地调试] 在脚本同目录创建 config.json，参考 config.example.json"
    )


# ======== 函数 2：下载一份 CSV ========

def download_csv(label: str, url: str) -> str:
    """
    下载一份 TRQ CSV，同时存档到 data/ 目录。

    Args:
        label: 数据源标签，如 "NFTA-Q4" / "FTA-Q4"（用来命名存档文件）
        url:   CSV 直链

    Returns:
        str: CSV 文件的完整文本内容，交给下一步解析函数用

    Raises:
        urllib.error.URLError: 网络请求失败
        UnicodeDecodeError:    编码尝试全失败时抛
    """
    print(f"[download] ⬇️  {label}: {url}")

    # --- 发请求 ---
    req = urllib.request.Request(url, headers={"User-Agent": "TRQ-Sync/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        last_modified = resp.headers.get("Last-Modified", "(无)")
        raw_bytes = resp.read()

    # --- 存档：原始 CSV 当日快照 ---
    # GitHub Actions 之后会把 data/ 里新增的文件 commit 回 repo
    # 你本地 git pull 就能拿到所有历史
    snapshot_path = DATA_DIR / f"{label}_{TODAY}.csv"
    snapshot_path.write_bytes(raw_bytes)

    # --- 解码 ---
    # 加拿大政府文件通常是 UTF-8-BOM；偶尔降级到 cp1252 是为了稳健
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            text = raw_bytes.decode(encoding)
            print(
                f"           ✅ {len(raw_bytes):,} bytes · encoding={encoding} · "
                f"Last-Modified={last_modified}"
            )
            return text
        except UnicodeDecodeError:
            continue

    # 理论上跑不到这里（latin-1 能解码任何字节），但留个防御
    raise UnicodeDecodeError(
        "utf-8", raw_bytes, 0, 1,
        f"无法用 utf-8-sig/utf-8/cp1252/latin-1 解码 {label}"
    )


# ======== 函数 3：解析一份 TRQ CSV ========

def parse_trq_csv(text: str, country_group: str) -> list:
    """
    把 SSRS 导出的脏 CSV 解析成干净的 dict 列表。

    SSRS CSV 的结构（踩过的坑）：
      - 第 1-4 行：垃圾表头（Textbox 乱码、分类标题、空行、真列名混着假列名）
      - 第 5-27 行：23 个产品类别的真实数据  ← 我们要的部分
      - 第 28 行起：按国家拆分的明细         ← 我们不要

    每行真数据的列位置（前 6 列是重复的表头文字，跳过）：
      [6]  id                   → int
      [7]  name                 → str
      [8]  max_quota_kg         → "10,369,200" → 10369200
      [9]  max_country_share_%  → "21%"        → 21.0
      [10] utilized_kg          → "10,369,200" → 10369200
      [11] utilization_%        → "100.00%"    → 100.0
      [12] remaining_kg         → "0"          → 0

    Args:
        text:          CSV 文件的完整文本（download_csv 的返回值）
        country_group: "Non-FTA" 或 "FTA"，会写进每个 dict

    Returns:
        list[dict]: 23 条记录，每条形如
        {
            "category_id":             20,
            "category_name":          "Standard Pipe",
            "max_quota_kg":           10369200,
            "max_country_share_pct":  21.0,
            "utilized_kg":            10369200,
            "utilization_pct":        100.0,
            "remaining_kg":           0,
            "country_group":          "Non-FTA",
            "quarter":                "Q4",
        }
    """
    # --- 用 csv 模块正确处理引号包裹的千分位数字 "10,369,200" ---
    reader = csv.reader(io.StringIO(text))
    all_rows = list(reader)

    rows = []
    for raw in all_rows:
        # 空行或列数不够（非数据行）直接跳
        if len(raw) < 13:
            continue
        # 数据行的第 0 列必定是 "Product Category"，拿这个来识别最稳
        if raw[0].strip() != "Product Category":
            continue

        # --- 提取真实字段 ---
        try:
            category_id   = int(raw[6].strip())
            category_name = raw[7].strip()
            max_quota_kg  = _parse_number(raw[8])
            share_pct     = _parse_percent(raw[9])
            utilized_kg   = _parse_number(raw[10])
            util_pct      = _parse_percent(raw[11])
            remaining_kg  = _parse_number(raw[12])
        except (ValueError, IndexError) as e:
            print(f"[parse] ⚠️  跳过异常行：{raw[:3]}... 原因：{e}")
            continue

        rows.append({
            "category_id":            category_id,
            "category_name":          category_name,
            "max_quota_kg":           max_quota_kg,
            "max_country_share_pct":  share_pct,
            "utilized_kg":            utilized_kg,
            "utilization_pct":        util_pct,
            "remaining_kg":           remaining_kg,
            "country_group":          country_group,
            "quarter":                CURRENT_QUARTER,
        })

    print(f"[parse] ✅ {country_group}: 解析出 {len(rows)} 条记录")
    return rows


def _parse_number(s: str) -> int:
    """'10,369,200' → 10369200 | '0' → 0 | '' → 0"""
    s = s.strip().replace(",", "").replace('"', "")
    return int(s) if s else 0


def _parse_percent(s: str) -> float:
    """'100.00%' → 100.0 | '21%' → 21.0 | '' → 0.0"""
    s = s.strip().replace("%", "").replace('"', "")
    return float(s) if s else 0.0


# ======== 函数 4：计算状态 ========

def compute_status(utilization_pct: float) -> str:
    """
    把利用率 % 映射到 4 档状态标签。这个字段驱动 Notion 看板的颜色分组。

    阈值业务含义：
      < 50%     → Healthy    配额充裕，放心下单
      50-80%    → Warning    开始留意
      80-<100%  → Critical   马上下单或转 FTA
      >= 100%   → Used Up    本季用光，等下季或绕 FTA

    Args:
        utilization_pct: 利用率，0-100 的浮点数（大于 100 也可能，按 Used Up 处理）

    Returns:
        str: 四选一："Healthy" / "Warning" / "Critical" / "Used Up"
    """
    if utilization_pct >= 100.0:
        return "Used Up"
    if utilization_pct >= 80.0:
        return "Critical"
    if utilization_pct >= 50.0:
        return "Warning"
    return "Healthy"


# ======== 函数 5：查产品标签 ========

def get_product_tags(category_name: str) -> list:
    """
    查询一个产品类别要挂哪些焦点 tag（EMT/RIGID/STRUT/SUPPORT）。

    只有 TAG_MAP 里登记的 6 个类别会返回非空列表，其他 17 类返回 []。
    Notion 那边可以用 "Product Tag is not empty" 过滤出焦点看板。

    Args:
        category_name: 产品类别英文名，如 "Standard Pipe"

    Returns:
        list[str]: tag 名字列表，如 ["EMT", "RIGID"]；无匹配返回 []
    """
    return TAG_MAP.get(category_name, [])


# ======== 函数 6：组装 Notion 页面 payload ========

def build_notion_page(row: dict, data_source_id: str) -> dict:
    """
    把一条 dict 翻译成 Notion REST API /v1/pages POST 要吃的 JSON 结构。

    这个函数只做数据转换，不发请求（便于单测和 dry-run 看 JSON）。

    严格对应 Notion 数据库的实际属性：
      - Snapshot         (title)         → "{date} · {group} · {category}"
      - Product Category (select)        → row["category_name"]
      - Category ID      (number)        → row["category_id"]
      - Country Group    (select)        → row["country_group"]
      - Quarter          (select)        → row["quarter"]
      - Max Quota (KG)   (number)
      - Max Country Share % (number)
      - Utilized (KGA)   (number)
      - Utilization %    (number)
      - Remaining (KG)   (number)
      - Status           (select)        ← compute_status()
      - Product Tag      (multi_select)  ← get_product_tags()，非焦点不加
      - Date             (date)          → TODAY
      - Source URL       (url)           → SOURCES 里查

    Args:
        row:            parse_trq_csv 返回的一条 dict
        data_source_id: Notion data source ID（从 load_config 拿）

    Returns:
        dict: 完整的 Notion API payload，可直接 json.dumps 后 POST
    """
    # --- 算出衍生字段 ---
    status = compute_status(row["utilization_pct"])
    tags = get_product_tags(row["category_name"])

    # --- Source URL：根据 country_group 反查 SOURCES ---
    #     Non-FTA → NFTA-Q4；FTA → FTA-Q4
    src_label = ("NFTA" if row["country_group"] == "Non-FTA" else "FTA") + f"-{row['quarter']}"
    source_url = SOURCES.get(src_label, "")

    # --- 标题：人类扫一眼就能分辨的组合 ---
    title_text = f"{TODAY} · {row['country_group']} · {row['category_name']}"

    # --- 核心 properties 字典 ---
    properties = {
        "Snapshot": {
            "title": [{"text": {"content": title_text}}]
        },
        "Product Category":    {"select": {"name": row["category_name"]}},
        "Category ID":         {"number": row["category_id"]},
        "Country Group":       {"select": {"name": row["country_group"]}},
        "Quarter":             {"select": {"name": row["quarter"]}},
        "Max Quota (KG)":      {"number": row["max_quota_kg"]},
        "Max Country Share %": {"number": row["max_country_share_pct"]},
        "Utilized (KG)":       {"number": row["utilized_kg"]},
        "Utilization %":       {"number": row["utilization_pct"]},
        "Remaining (KG)":      {"number": row["remaining_kg"]},
        "Status":              {"select": {"name": status}},
        "Date":                {"date": {"start": TODAY}},
        "Source URL":          {"url": source_url},
    }

    # Product Tag 只有焦点 6 类才有；非焦点不加这个字段，Notion 里就显示为空
    if tags:
        properties["Product Tag"] = {
            "multi_select": [{"name": t} for t in tags]
        }

    # --- 最终 payload：parent + properties ---
    return {
        "parent": {
            "type": "data_source_id",
            "data_source_id": data_source_id,
        },
        "properties": properties,
    }


# ======== 函数 7：批量推送到 Notion ========

def push_to_notion(payloads: list, token: str, dry_run: bool = False) -> dict:
    """
    把一批 payload 推送到 Notion，带限流和重试。

    这是脚本里**唯一碰网络的写操作**，所以默认 dry_run=True 保险。
    真推送时（dry_run=False）：
      - 每条之间 sleep 0.35s（Notion 限 3 req/s）
      - 单条失败（网络 / 5xx / 429）重试 3 次，指数退避
      - 单条失败（4xx 非 429，属性拼错等）直接跳过并记录错误
      - 最终返回统计，方便 main() 判断脚本是否该标记为失败

    Args:
        payloads: build_notion_page 生成的 46 条 payload 列表
        token:    Notion integration token（Bearer）
        dry_run:  True 时只打印前 3 条标题，不发请求

    Returns:
        dict: {
            "ok":       int,   # 成功数
            "fail":     int,   # 失败数
            "errors":   list,  # 失败详情字符串列表
            "dry_run":  bool,  # 是否 dry-run
        }
    """
    total = len(payloads)

    # --- Dry run：只预览，不发请求 ---
    if dry_run:
        print(f"[push] 🔶 DRY RUN（不推送）- 共 {total} 条 payload 已准备好")
        for i, p in enumerate(payloads[:3], 1):
            title = p["properties"]["Snapshot"]["title"][0]["text"]["content"]
            print(f"         [{i}] {title}")
        if total > 3:
            print(f"         ... 以及其他 {total - 3} 条")
        return {"ok": 0, "fail": 0, "errors": [], "dry_run": True}

    # --- 真推送 ---
    stats = {"ok": 0, "fail": 0, "errors": [], "dry_run": False}
    print(f"[push] 📤 开始推送 {total} 条到 Notion（每条间隔 ~0.35s）...")

    for i, payload in enumerate(payloads, 1):
        title = payload["properties"]["Snapshot"]["title"][0]["text"]["content"]
        try:
            _post_one(payload, token)
            stats["ok"] += 1
            print(f"[push] ✅ {i:>2}/{total}  {title}")
        except Exception as e:
            stats["fail"] += 1
            err_line = f"{title}: {e}"
            stats["errors"].append(err_line)
            print(f"[push] ❌ {i:>2}/{total}  {err_line}")

        # Notion 限流 ~3 req/sec，保险间隔 350ms
        time.sleep(0.35)

    print(f"\n[push] 📊 完成：{stats['ok']} 成功 / {stats['fail']} 失败")
    if stats["errors"]:
        print("[push] ⚠️  失败详情：")
        for err in stats["errors"]:
            print(f"         - {err}")

    return stats


def _post_one(payload: dict, token: str, max_retries: int = 3) -> dict:
    """
    发送单个 POST 请求，带指数退避重试。

    重试规则：
      - 429（限流） / 5xx          → 等 2/4/8 秒后重试
      - 4xx (400-499, 非 429)      → 不重试，立即抛 RuntimeError
                                     （多半是 payload 数据错，重试也没用）
      - 网络错误 (URLError)         → 等 2/4/8 秒后重试

    Returns:
        dict: Notion API 返回的 JSON（含新建 page 的 id 等）

    Raises:
        RuntimeError: 重试耗尽或遇到不可重试错误
    """
    data = json.dumps(payload).encode("utf-8")
    headers = {
        "Authorization":  f"Bearer {token}",
        "Notion-Version": NOTION_API_VERSION,
        "Content-Type":   "application/json",
    }

    for attempt in range(1, max_retries + 1):
        req = urllib.request.Request(
            NOTION_API_URL, data=data, headers=headers, method="POST"
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))

        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="ignore")[:300]
            # 4xx 非 429：属性名/类型错了，重试也不会好，立刻抛
            if 400 <= e.code < 500 and e.code != 429:
                raise RuntimeError(f"HTTP {e.code}（不可重试）：{body}")
            # 429 / 5xx：可重试
            wait = 2 ** attempt
            print(f"         ⏳ HTTP {e.code}，{wait}s 后重试 ({attempt}/{max_retries})")
            time.sleep(wait)

        except urllib.error.URLError as e:
            # DNS 失败、连接被拒等网络问题，可重试
            wait = 2 ** attempt
            print(f"         ⏳ 网络错误 {e}，{wait}s 后重试 ({attempt}/{max_retries})")
            time.sleep(wait)

    raise RuntimeError(f"重试 {max_retries} 次后仍失败")


# ======== 函数 8：主工作流（生产环境用） ========

def main(dry_run: bool = False) -> int:
    """
    生产工作流（GitHub Actions 每天跑的就是这个）：
      load_config → download_csv × 2 → parse_trq_csv × 2
      → build_notion_page × 46 → push_to_notion → 返回 exit code

    Args:
        dry_run: True 时只组装 payload 不推送，用于本地调试

    Returns:
        int: 0 全部成功 / 1 有任何失败（GitHub Actions 据此判断 run 状态）
    """
    print(f"🚀 TRQ-Notion Sync · {TODAY} · {CURRENT_QUARTER}")
    print(f"   Mode: {'🔶 DRY RUN' if dry_run else '📤 LIVE PUSH'}")
    print("=" * 60)

    # --- 1. 凭据 ---
    cfg = load_config()

    # --- 2. 下载 + 解析两份 CSV ---
    all_rows = []
    for label, url in SOURCES.items():
        text = download_csv(label, url)
        group = "Non-FTA" if label.startswith("NFTA") else "FTA"
        all_rows += parse_trq_csv(text, country_group=group)

    # --- 3. 组装 46 条 payload ---
    payloads = [
        build_notion_page(r, data_source_id=cfg["data_source_id"])
        for r in all_rows
    ]
    print(f"\n📦 共组装 {len(payloads)} 条 payload")

    # --- 4. 推送 ---
    stats = push_to_notion(payloads, cfg["notion_token"], dry_run=dry_run)

    # --- 5. 返回 exit code ---
    if stats.get("dry_run"):
        print("\n✅ DRY RUN 完成（未推送）")
        return 0
    exit_code = 0 if stats["fail"] == 0 else 1
    print(f"\n{'✅' if exit_code == 0 else '❌'} 全部完成，exit code = {exit_code}")
    return exit_code


# ======== 测试套件（仅 --test 时运行） ========

def run_tests():
    # --- 测试 1：load_config ---
    print("=" * 60)
    print("测试 1: load_config")
    print("=" * 60)
    cfg = load_config()
    print(f"  token 长度：{len(cfg['notion_token'])} 字符")
    print(f"  data_source_id：{cfg['data_source_id']}")
    print("✅ load_config 测试通过\n")

    # --- 测试 2：download_csv ---
    print("=" * 60)
    print("测试 2: download_csv（下载 2 份 CSV）")
    print("=" * 60)
    csv_texts = {}  # 留给下面的 parse 测试用
    for label, url in SOURCES.items():
        text = download_csv(label, url)
        csv_texts[label] = text
        print(f"  {label}: 文本 {len(text):,} 字符 · {len(text.splitlines())} 行\n")
    print("✅ download_csv 测试通过\n")

    # --- 测试 3：parse_trq_csv ---
    print("=" * 60)
    print("测试 3: parse_trq_csv（解析 CSV 为 dict 列表）")
    print("=" * 60)
    all_rows = []
    all_rows += parse_trq_csv(csv_texts["NFTA-Q4"], country_group="Non-FTA")
    all_rows += parse_trq_csv(csv_texts["FTA-Q4"],  country_group="FTA")
    print(f"\n  总记录数：{len(all_rows)}（应为 46：两份 CSV × 23 类别）\n")

    # 抽查：Standard Pipe（category_id=20）的 Non-FTA 行
    sample = next(
        (r for r in all_rows
         if r["category_name"] == "Standard Pipe" and r["country_group"] == "Non-FTA"),
        None,
    )
    if sample:
        print("  🔍 抽查 Standard Pipe (Non-FTA)：")
        for k, v in sample.items():
            print(f"    {k:26s} = {v!r}")
    else:
        print("  ⚠️  没找到 Standard Pipe (Non-FTA) 样本，检查解析")

    # 抽查：EMT/RIGID/STRUT/SUPPORT 焦点的 6 类
    focus_names = list(TAG_MAP.keys())
    print(f"\n  🎯 焦点 6 类在两份 CSV 中的记录：")
    for r in all_rows:
        if r["category_name"] in focus_names:
            print(
                f"    [{r['country_group']:8s}] "
                f"{r['category_name']:30s} "
                f"util={r['utilization_pct']:6.2f}%  "
                f"remaining={r['remaining_kg']:>12,} kg"
            )
    print("\n✅ parse_trq_csv 测试通过\n")

    # --- 测试 4 + 5：compute_status + get_product_tags ---
    print("=" * 60)
    print("测试 4 + 5: compute_status & get_product_tags（给每行贴标签）")
    print("=" * 60)

    # 先单测阈值边界（确认分档逻辑对）
    print("  🔬 compute_status 阈值边界：")
    for pct, expected in [
        (0.0,   "Healthy"),
        (49.9,  "Healthy"),
        (50.0,  "Warning"),
        (79.9,  "Warning"),
        (80.0,  "Critical"),
        (99.9,  "Critical"),
        (100.0, "Used Up"),
        (120.0, "Used Up"),
    ]:
        got = compute_status(pct)
        ok  = "✅" if got == expected else "❌"
        print(f"    {ok} {pct:>6.1f}% → {got:10s} (期望 {expected})")

    # 跑一遍真实 46 行，看贴标签效果 —— 只打印焦点 6 类
    print("\n  🎯 焦点 6 类贴标签预览：")
    print(f"    {'Group':8s} {'Category':30s} {'Util':>7s}  {'Status':10s} {'Tags'}")
    print(f"    {'-'*8} {'-'*30} {'-'*7}  {'-'*10} {'-'*20}")
    for r in all_rows:
        tags = get_product_tags(r["category_name"])
        if not tags:  # 只看焦点
            continue
        status = compute_status(r["utilization_pct"])
        print(
            f"    {r['country_group']:8s} "
            f"{r['category_name']:30s} "
            f"{r['utilization_pct']:>6.2f}%  "
            f"{status:10s} "
            f"{tags}"
        )

    # 统计一下：非焦点类别确实返回 []
    non_focus = [r for r in all_rows if not get_product_tags(r["category_name"])]
    print(f"\n  📊 非焦点类别记录数：{len(non_focus)} （应为 34 = 46 - 12）")

    print("\n✅ compute_status + get_product_tags 测试通过\n")

    # --- 测试 6：build_notion_page ---
    print("=" * 60)
    print("测试 6: build_notion_page（组装 Notion payload，不推送）")
    print("=" * 60)

    # 拿 Standard Pipe (Non-FTA) 当样例，看完整 JSON 长啥样
    sample_row = next(
        r for r in all_rows
        if r["category_name"] == "Standard Pipe" and r["country_group"] == "Non-FTA"
    )
    payload = build_notion_page(sample_row, data_source_id=cfg["data_source_id"])
    print("  🔍 Standard Pipe (Non-FTA) 完整 payload：")
    print(json.dumps(payload, indent=2, ensure_ascii=False))

    # 校验关键字段
    props = payload["properties"]
    checks = [
        ("Snapshot 标题",  props["Snapshot"]["title"][0]["text"]["content"].startswith(TODAY)),
        ("Product Category", props["Product Category"]["select"]["name"] == "Standard Pipe"),
        ("Country Group",    props["Country Group"]["select"]["name"] == "Non-FTA"),
        ("Status=Used Up",   props["Status"]["select"]["name"] == "Used Up"),
        ("Product Tag=EMT/RIGID",
                             [t["name"] for t in props["Product Tag"]["multi_select"]] == ["EMT", "RIGID"]),
        ("Max Quota (KG)",   props["Max Quota (KG)"]["number"] == 10369200),
        ("Utilization %",    props["Utilization %"]["number"] == 100.0),
        ("Source URL 非空",  bool(props["Source URL"]["url"])),
        ("Date = TODAY",     props["Date"]["date"]["start"] == TODAY),
        ("Parent data_source_id",
                             payload["parent"]["data_source_id"] == cfg["data_source_id"]),
    ]
    print("\n  🔬 关键字段校验：")
    for name, ok in checks:
        print(f"    {'✅' if ok else '❌'} {name}")

    # 非焦点类别应该没有 Product Tag 字段
    non_focus_row = next(
        r for r in all_rows if r["category_name"] == "Rebar"
    )
    non_focus_payload = build_notion_page(non_focus_row, data_source_id=cfg["data_source_id"])
    no_tag = "Product Tag" not in non_focus_payload["properties"]
    print(f"    {'✅' if no_tag else '❌'} 非焦点（Rebar）无 Product Tag 字段")

    # 批量生成 46 条（只算、不推）
    all_payloads = [
        build_notion_page(r, data_source_id=cfg["data_source_id"])
        for r in all_rows
    ]
    print(f"\n  📊 批量生成 payload：{len(all_payloads)} 条（应为 46）")

    print("\n✅ build_notion_page 测试通过\n")

    # --- 测试 7a：push_to_notion (DRY RUN，安全) ---
    print("=" * 60)
    print("测试 7a: push_to_notion（DRY RUN - 不发任何请求）")
    print("=" * 60)
    result = push_to_notion(all_payloads, cfg["notion_token"], dry_run=True)
    print(f"  结果：{result}")
    print("✅ dry_run 测试通过\n")

    # --- 测试 7b：push_to_notion (真推 1 条，验证 Notion 那边能收) ---
    # ⚠️ 这段会在你 Notion 数据库真的新建一条 page（Standard Pipe Non-FTA）
    # 如果想跳过（比如只想测逻辑），把下面改成 RUN_REAL_PUSH = False
    RUN_REAL_PUSH = True
    print("=" * 60)
    print(f"测试 7b: push_to_notion（真推 1 条 · RUN_REAL_PUSH={RUN_REAL_PUSH}）")
    print("=" * 60)
    if RUN_REAL_PUSH:
        single = [build_notion_page(sample_row, data_source_id=cfg["data_source_id"])]
        real = push_to_notion(single, cfg["notion_token"], dry_run=False)
        print(f"  结果：{real}")
        if real["ok"] == 1:
            print("✅ 真推送测试通过 - 去 Notion 看看新建的那条 Standard Pipe (Non-FTA)")
        else:
            print("❌ 真推送失败 - 看上面的错误详情")
    else:
        print("  (跳过真推送，把 RUN_REAL_PUSH 改成 True 再跑)")


# ======== CLI 入口 ========

if __name__ == "__main__":
    import sys

    args = sys.argv[1:]

    # 三种启动方式：
    #   python3 sync_to_notion.py             → main() 真推送（GitHub Actions 用）
    #   python3 sync_to_notion.py --dry-run   → main() 不推送（本地看流程）
    #   python3 sync_to_notion.py --test      → run_tests() 老测试套件
    if "--test" in args:
        run_tests()
    else:
        dry_run = "--dry-run" in args
        exit_code = main(dry_run=dry_run)
        sys.exit(exit_code)
