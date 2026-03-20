# openclaw-skills

机器人数据运营工具集，当前包含四个部分：

- `scripts/sample_deliver`：从 Lightwheel 平台下载打包完成的数据，并生成 Excel 交付报告
- `skills/case-copy`：按质检状态批量复制 human case 到目标项目，并生成复制报告
- `skills/pipeline-monitor`：工作流链路监控 skill，支持定时监控和交互查询
- `skills/daily-report`：运营日报 skill，从 Clickhouse 和数据平台 API 拉取数据并生成 Markdown 日报

## 目录结构

```text
.
├── scripts/
│   ├── pyproject.toml
│   └── sample_deliver/
│       ├── __init__.py
│       ├── api.py
│       ├── downloader.py
│       ├── report.py
│       └── tool.py
└── skills/
    ├── case-copy/
    │   ├── SKILL.md
    │   └── scripts/
    │       ├── test_tool.py
    │       └── tool.py
    ├── daily-report/
    │   ├── SKILL.md
    │   └── scripts/
    │       └── query.py
    └── pipeline-monitor/
        ├── SKILL.md
        ├── config.example.json
        └── scripts/
            ├── monitor.py
            ├── query.py
            └── test_monitor.py
```

## scripts/sample_deliver

### 功能

`sample_deliver` 会：

1. 查询指定项目里 `complete_job` 且 `nodeStatus=3` 的 case
2. 下载 `metadata_trim.json`、`task.zip`、`episode.mcap`、`episode_vis.mcap`
3. 从 metadata 中解析 `episode_uuid` 和真实 `task_name`
4. 按 task / episode 组织输出目录
5. 生成 Excel 交付报告

### 安装

远程安装：

```bash
pip install git+https://github.com/wenjiegu620-rgb/lwoptools.git#subdirectory=scripts
```

本地安装：

```bash
cd scripts
pip install -e .
```

### 直接调用

```python
from sample_deliver import download_project_data

result = download_project_data(
    project_id="your-project-uuid",
    username="your.name",
    token="eyJ...",
    limit=10,
    output_dir="./downloads",
    env="prod",
    max_speed_mbps=0,
)

print(result["success"])
print(result["report_path"])
print(result["tasks"])
```

### 作为 Agent Tool 使用

```python
from langchain.tools import tool
from sample_deliver import download_project_data

download_tool = tool(download_project_data)
```

### 参数说明

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `project_id` | `str` | 无 | Lightwheel 平台项目 UUID |
| `username` | `str` | 无 | 平台用户名 |
| `token` | `str` | 无 | Bearer token，可带或不带 `Bearer` 前缀 |
| `limit` | `int` | `0` | 下载数量上限，`0` 表示全部 |
| `output_dir` | `str` | `./downloads` | 本地输出目录 |
| `env` | `str` | `prod` | `prod` 或 `dev` |
| `max_speed_mbps` | `float` | `0.0` | 下载限速，单位 MB/s，`0` 表示不限速 |

### 返回值

`download_project_data()` 返回：

```json
{
  "success": true,
  "total_available": 42,
  "downloaded": 10,
  "num_tasks": 3,
  "tasks": ["pick_apple", "pour_water", "open_drawer"],
  "report_path": "/abs/path/delivery_report_20260319_130000.xlsx",
  "output_dir": "/abs/path/downloads",
  "error": null
}
```

注意：

- `total_available` 当前表示经过 `limit` 截断后实际进入下载流程的 case 数
- 单个 case 下载失败时不会中断整个批次，而是跳过后继续处理

### 输出目录

```text
downloads/
├── {task_name}/
│   ├── {episode_uuid}/
│   │   └── ... task.zip 解压后的文件
│   ├── {episode_uuid}.mcap
│   └── {episode_uuid}_vis.mcap
└── delivery_report_YYYYMMDD_HHMMSS.xlsx
```

### Excel 报告内容

- `总览`：项目 ID、下载时间、数量统计、Task 列表、输出目录
- `文件清单`：每个文件对应的 `task_name`、`episode_uuid`、相对文件路径

## skills/case-copy

### 功能

`case-copy` 用于从源项目筛出 `human_case_inspect` 节点下指定质检状态的 human case，并批量复制到目标项目。脚本会分别查询：

- `nodeStatus=3`：质检通过
- `nodeStatus=4`：质检不通过

复制完成后会生成 Excel 报告，包含每条 case 的：

- case 基本信息
- 原始质检状态
- 复制结果状态
- 执行时间

当复制接口未返回逐条成功/失败明细时，报告会明确标记为“已提交复制，接口未返回逐条结果”，避免把请求成功误写成复制成功。

### 依赖

```bash
pip3 install requests pandas openpyxl loguru
```

### 使用

```bash
python3 skills/case-copy/scripts/tool.py
```

脚本会依次提示输入：

1. 用户名
2. Bearer token
3. 源项目 UUID
4. 目标项目 UUID
5. 每种状态复制条数
6. 输出 Excel 文件名
7. 环境 `prod` / `dev`

环境输入支持大小写，非法值会直接给出明确错误提示。

### 输出

脚本会输出：

- 已确认复制成功的数量
- 失败或跳过的数量
- 待人工确认的数量
- Excel 报告路径

### 测试

```bash
python3 -m unittest skills/case-copy/scripts/test_tool.py
```

覆盖点包括：

- 环境参数校验与大小写兼容
- 复制接口返回部分成功/失败时的汇总逻辑
- 接口未返回逐条结果时的保守标记逻辑
- Excel 报告行的 `copy_status` 标记

## skills/pipeline-monitor

### 功能

`pipeline-monitor` 用于监控工作流链路中的失败堆积，并输出适合飞书发送的 Markdown 消息。包含两种模式：

- 定时监控：执行 `monitor.py`，判断是否达到预警/报警阈值
- 交互查询：执行 `query.py`，查询项目状态、失败 case 详情或最近 7 天趋势

### 安装与部署

```bash
mkdir -p ~/.openclaw/skills
cp -r skills/pipeline-monitor ~/.openclaw/skills/
cp skills/pipeline-monitor/config.example.json ~/.openclaw/skills/pipeline-monitor/config.json
pip install clickhouse-driver
```

### 配置项

配置文件路径：

```text
~/.openclaw/skills/pipeline-monitor/config.json
```

主要字段：

- `alert.observe_threshold` / `observe_rate`：观察级阈值
- `alert.warn_threshold` / `warn_growth`：预警阈值
- `alert.critical_threshold` / `critical_consecutive`：报警阈值
- `alert.silence_hours`：同一节点报警后的静默时长
- `feishu_group_id`：飞书群 ID
- `node_owners`：节点名到飞书 user_id 的映射
- `monitored_projects`：监控项目范围，支持 `"all"` 或关键字列表
- `clickhouse.*`：ClickHouse 连接信息

### 定时监控

```bash
python3 ~/.openclaw/skills/pipeline-monitor/scripts/monitor.py
```

脚本会输出 JSON，例如：

```json
{
  "type": "monitor_result",
  "feishu_group_id": "oc_xxx",
  "has_alert": true,
  "alert_count": 2,
  "message": "...markdown...",
  "alerts": []
}
```

其中：

- `has_alert=true` 时，可将 `message` 发送到飞书群
- `has_alert=false` 时通常静默，不发送消息
- 本地快照会写入 `skills/pipeline-monitor/snapshots/latest.json`

### 交互查询

```bash
python3 ~/.openclaw/skills/pipeline-monitor/scripts/query.py --project DM_sample_0223 --mode status
python3 ~/.openclaw/skills/pipeline-monitor/scripts/query.py --project DM_sample_0223 --node data_cut --mode detail
python3 ~/.openclaw/skills/pipeline-monitor/scripts/query.py --project DM_sample_0223 --mode trend
```

支持模式：

- `status`：查看项目下各节点失败堆积概览
- `detail`：查看某个节点最近失败 case 列表，必须带 `--node`
- `trend`：查看最近 7 天失败趋势

输出是 JSON，真正需要发送给用户的是其中的 `message` 字段。

### 报警逻辑

| 级别 | 条件 | 动作 |
|------|------|------|
| 观察 | 失败堆积 > 10 且 1h 失败率 > 15% | 仅记录，不发消息 |
| 预警 | 失败堆积 >= 30 且 1h 增量 >= 20 | 飞书群发消息，不 @ |
| 报警 | 失败堆积 >= 50 且连续 2 次检测增长 | 飞书群发消息，并 @ 负责人 |

同一节点报警后会进入静默期，避免重复打扰。

### 测试

仓库当前包含 `case-copy` 和 `pipeline-monitor` 的单元测试：

```bash
python3 -m unittest skills/case-copy/scripts/test_tool.py
python3 -m unittest skills/pipeline-monitor/scripts/test_monitor.py
```

覆盖点包括：

- `case-copy` 的环境参数校验与复制结果汇总
- `is_in_silence()` 的静默期边界
- `check_alerts()` 的连续增长与静默逻辑
- `query.py` / `monitor.py` 的 SQL 参数化

## skills/daily-report

### 功能

`daily-report` 用于生成运营日报，输出 Markdown 文本，适合直接贴到群里或日报系统。当前日报包含三块：

- 采集与质检：按客户 × 设备维度汇总今日采集完成、累计采集完成、待质检、质检通过、人效、采集人数
- 标注进度：按语义版本汇总采集流入标注、语义标注完成、手势标注完成、标注完成、吞吐率
- 采集供应商明细：按供应商汇总今日采集完成时长、采集人数和人效

### 依赖

- 公司内网或 VPN
- Clickhouse 访问权限
- 数据平台 token

### 配置

脚本默认从下面路径读取 token：

```text
skills/daily-report/config.json
```

可以从仓库模板开始：

```bash
cp skills/daily-report/config.example.json skills/daily-report/config.json
```

文件格式：

```json
{
  "token": "your-auth-token",
  "clickhouse": {
    "host": "YOUR_CLICKHOUSE_HOST",
    "port": 9000,
    "database": "asset",
    "user": "YOUR_CLICKHOUSE_USER",
    "password": "YOUR_CLICKHOUSE_PASSWORD"
  }
}
```

也可以运行时通过 `--token` 临时传入；ClickHouse 连接信息还支持通过环境变量覆盖：

- `DAILY_REPORT_CH_HOST`
- `DAILY_REPORT_CH_PORT`
- `DAILY_REPORT_CH_DB`
- `DAILY_REPORT_CH_USER`
- `DAILY_REPORT_CH_PASS`

### 使用

生成今日日报：

```bash
python3 skills/daily-report/scripts/query.py
```

生成指定日期日报：

```bash
python3 skills/daily-report/scripts/query.py --date 2026-03-17
```

临时指定 token：

```bash
python3 skills/daily-report/scripts/query.py --date 2026-03-17 --token "<jwt>"
```

### 输出

脚本输出 Markdown，包含：

- `运营日报 · YYYY-MM-DD`
- `一、采集 & 质检（今日）`
- `二、标注进度（今日）`
- `附：今日采集供应商明细`

### 指标特点

- 采集人数按客户 × 设备维度跨项目去重
- 多数指标按 `data_uuid` 和最新 `workflow_run_id` 去重，避免重跑重复计入
- 标注完成相关指标按首次 success 统计，重刷不重复计入

### 当前限制

- 仓库中暂未提供 `daily-report` 的自动化测试
- 脚本依赖真实内网环境，离线环境下无法本地验证查询结果

## 说明

当前仓库没有为 `sample_deliver` 提供自动化测试；如果后续继续迭代，建议优先补上对 API 响应解析、目录输出结构和报告生成的回归测试。
