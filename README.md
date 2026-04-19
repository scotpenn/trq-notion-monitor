# Canada Steel TRQ / SURTAX Monitor

## 做什么

每日自动抓取加拿大钢铁进口 TRQ（关税配额）使用率数据，聚焦 EMT/RSC（HS 7306.30.00.xx）相关类别，写入 Notion 信息板，用于判断订单下单窗口。

## 目标场景

- **OPP-2026-017（Modern Niagara / Nick）** 需要判断 EMT 订单下单时机，避开配额用尽期
- 监控 Non-FTA（中国）和 FTA（Mexico / Korea / Vietnam）两套配额使用率对比
- 预警：使用率 >80%、剩余配额 <30 天耗尽速度 → 红色卡片

## 数据源（权威）

Global Affairs Canada 发布的钢铁 TRQ 使用率报告，CSV/XLSX 格式：

- Non-FTA（中国、印度等）：`https://www.eics-scei.gc.ca/report-rapport/TRQ_NFTA-Q{1|2|3|4}.csv`
- FTA（USMCA 除外的墨西哥、韩国等）：`https://www.eics-scei.gc.ca/report-rapport/TRQ_FTA-Q{1|2|3|4}.csv`

**当前季度**：Q4（2026-03-26 至 2026-06-27）

## 建设步骤

| Step | 动作 | 状态 |
|------|------|------|
| 1 | 下载并摸清 CSV 数据结构 / 更新频率 | ✅ 完成 2026-04-18 |
| 2 | 确认 EMT = Standard Pipe（类别 20） | ✅ 完成 2026-04-18 |
| 3 | 搭 Notion Dashboard（Expectation OS 内独立页面，可分享 partner） | 🔄 进行中 |
| 4 | 写正式抓取+同步脚本（分函数模块化，对接 Notion MCP） | ⏳ |
| 5 | 配置定时任务每日自动运行 | ⏳ |

## Step 1 关键结论（2026-04-18）

- **数据源确认每日更新**（两份 CSV Last-Modified 均为当天）
- **EMT → Standard Pipe**（TRQ 类别 20，HS 7306.30）
- **Q4 Standard Pipe 配额现状**：
  - Non-FTA（含中国）：**100% 用尽**，剩余 0 KG
  - FTA（含墨西哥）：**100% 用尽**，剩余 0 KG
  - 当季所有 EMT 进口（无论来源国）都要付 50% surtax
- **Q1 重置日**：2026-06-27（Nick 订单若能让清关日落在此后可免 surtax）

## 目录

```
TRQ-SURTAX-Monitor/
├── README.md                        # 本文件
├── step1_fetch_and_inspect.py       # 步骤 1：摸数据结构
├── data/                            # 下载的 CSV 原始文件（按日期快照）
└── notes/                           # 过程笔记
```

## 使用说明

当前处于 Step 1，需要 Scot 在本地执行：

```bash
cd "/Users/scotpan/Expectation System/16-金属大宗/TRQ-SURTAX-Monitor"
python3 step1_fetch_and_inspect.py
```

输出会保存到 `notes/step1_output_YYYY-MM-DD.txt`，同时原始 CSV 存到 `data/`。

把 `notes/step1_output_*.txt` 的内容发给 Claude，就可以进入 Step 2。
