# ThetaData History Downloader

Standalone downloader for unadjusted US stock/ETF and index 1-minute OHLCV history using the ThetaData Python SDK. Full-history downloads, monthly chunks, incremental updates, retry/session recovery, CSV/Parquet output, and offline validation. MIT licensed.

独立的美股股票、ETF 与指数一分钟历史行情下载器，从原回测项目中拆分。需要自己的 ThetaData 账户及相应历史数据订阅；仓库只发布代码，不包含账户凭据或行情数据。未复权价格，默认 `utp_cta`，不提供复权计算或回测功能。

## 安装

Python **3.12+**。建议使用全新虚拟环境：

```bash
git clone https://github.com/khakhasshi/thetadata-history-downloader.git
cd thetadata-history-downloader
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

使用 ThetaData SDK **1.0.12** 的远程认证及 gRPC 接口，无需启动本地 Theta Terminal。这与旧版 SDK/Terminal HTTP 下载流程不同。

## 认证

选择其中一种方式：

- 环境变量 `THETADATA_API_KEY`（由 SDK 读取），也可以将 `.env.example` 复制为本地 `.env` 后填写。
- 在仓库外保存凭据文件，第一行邮箱、第二行密码，通过 `--creds-file /path/to/creds.txt` 传入。建议将文件权限设置为 `600`。

真实凭据不应写入代码、命令行参数或提交到 Git。SDK 认证日志被屏蔽；重试及失败摘要仅记录异常类型，不记录服务器响应和原始异常内容。

## 首次下载

```bash
python scripts/batch_download_clean_1m.py \
  --symbols SPY QQQ NVDL \
  --start-date 2024-01-01 --end-date 2026-10-02 \
  --output-dir ./data/batch_1m --workers 4
```

有凭据文件时，在命令后追加 `--creds-file /path/to/creds.txt`。

附带当前研究使用的 60 个标的清单，仅是可修改的示例：

```bash
python scripts/batch_download_clean_1m.py \
  --symbols $(cat examples/us60.txt) \
  --start-date 2024-01-01 --end-date 2026-10-02 \
  --output-dir ./data/batch_1m --workers 4
```

按月分块请求；不存在数据的块会跳过。默认并行进程数 4；`--workers 1` 顺序下载并复用认证。默认从 1990 年开始；实际可获得数据取决于上市日期和账户权限。建议显式指定所需时间段。

## 增量更新

```bash
python scripts/update_1m_data_to_latest.py \
  --output-dir ./data/batch_1m --end-date 2026-10-02
```

默认更新目录中的全部标的；`--symbols SPY NVDL` 限定范围。

```bash
python scripts/update_1m_data_to_latest.py \
  --output-dir ./data/batch_1m \
  --add-symbols AMDL --new-symbol-start-date 2024-01-01 \
  --end-date 2026-10-02
```

增量更新顺序执行，读取已有 metadata 和数据，只请求已有覆盖之后的日期，再合并、排序、去重。空响应不会推进 metadata 的覆盖日期。支持整体移动数据目录；解析时优先使用新目录中的同名数据文件。

未指定结束日期时，取纽约**昨天或更早的最后一个工作日**，避免自动下载今天尚未收盘的行情。此计算未包含交易所节假日，不是交易日历；节假日可能无新增记录。建议显式指定最新完整交易日，并用验证器检查实际末日。不要使用尚未结束的交易日作为结束日期。

## 数据校验

```bash
python scripts/verify_downloads.py \
  --output-dir ./data/batch_1m --expected-end 2026-10-02
```

检查每个标的最新 metadata 对应的数据：非空、时间单调且唯一、OHLC 有效、非负有限成交量、固定时区，以及可选的实际末日。输出 JSON，失败退出码为 1。**不会补造缺失分钟，也不保证每个交易日都有 390 条记录**；停牌、稀疏成交、供应商缺失等需另行审计。

## 输出与时间口径

```text
data/batch_1m/
  SPY/
    SPY_1m_unadjusted_clean_edt_20240101_20261002.csv
    SPY_1m_unadjusted_clean_edt_20240101_20261002.parquet
    SPY_1m_metadata_20240101_20261002.json
  batch_summary_20240101_20261002.json
  incremental_update_summary_20261002.json
```

- 列由 SDK 返回，通常包括 `timestamp, open, high, low, close, volume, count, vwap`。
- 保留 SDK 时间戳的**纽约墙上时间**，移除原时区后附加固定 `Etc/GMT+4`（UTC−04:00）。例如冬季 09:30 仍保存为 `09:30-04:00`。这是既有回测器的兼容口径，**冬季不是正确的 UTC 瞬间**；不要将其用于跨时区对齐。可用 `--write-raw` 或 `--write-clean-local` 同时保留 SDK 原时区输出。
- 价格不复权。移除缺失、非有限或非正 OHLC 记录；违反 OHLC 边界或成交量有效性时失败。有效时间戳去重并排序。
- 文件名记录请求范围；实际覆盖以 metadata 的 `clean_edt_first_ts` / `clean_edt_last_ts` 和数据内容为准。成功 metadata 的 `end_date` 不超出实际最后日期，`requested_end_date` 保留请求值。
- CSV/Parquet 各自先写临时文件再替换；这**不是整个批次的事务**。单标的失败不会阻止其他标的继续，失败摘要使命令退出码为 1。`no_rows`、`no_new_rows` 并不证明请求区间已有完整覆盖，应运行验证器。
- 默认保留旧版本。`--force` 会重写同范围全量文件；增量 `--prune-old` 会删除旧版本，使用前自行备份。需要单一 Parquet 的回测器应先验证最新版本，再备份旧文件并接入。

## 常用选项

| 选项 | 说明 |
|---|---|
| `--asset-type stock` / `index` | 股票/ETF 或指数，权限由账户订阅决定 |
| `--interval 1m` | 当前发行版只支持一分钟线 |
| `--venue utp_cta` / `nqb` | 股票数据 venue；指数不使用此参数 |
| `--no-csv` / `--no-parquet` | 关闭一种输出格式，不可同时关闭 |
| `--retries` | 总尝试次数，默认 8 |
| `--retry-base-delay` / `--retry-max-delay` | 指数退避与抖动；每次 gRPC 请求上限 180 秒 |
| `--write-raw` / `--write-clean-local` | 全量下载时追加原始/原时区输出 |
| `--ranges-file` | 可选 JSON：`{"ranges":[{"symbol":"SPY","start_date":"20240101","end_date":"20261002"}]}` |

SDK 错误、网络/超时和失效 session 会按规则重试；权限不足和无数据不进行无效重试。只显示异常类型是主动的凭据保护，排查权限时先检查账户订阅和网络。`--help` 可查看各命令完整参数。

## 测试

```bash
python -m unittest discover -s tests -v
```

测试使用合成行情，不联网：月边界、符号路径校验、OHLC 清理、冬季固定时区语义、重试日志脱敏、目录迁移、空响应不推进日期、增量历史保持一致、验证器拒绝重复时间戳。GitHub Actions 配置 Python 3.12 / 3.13 / 3.14。

## 许可证

代码使用 [MIT](LICENSE)。ThetaData SDK 是独立依赖，其许可及供应商的数据订阅/再分发条款分别适用。仓库不提供行情再分发授权，与 ThetaData 官方无隶属关系。
