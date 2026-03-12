# sample-deliver

从 Lightwheel 平台下载打包完成的数据，并生成 Excel 交付报告。

## 安装

```bash
pip install git+https://github.com/wenjiegu620-rgb/sample-deliver.git
```

或本地安装：

```bash
git clone https://github.com/wenjiegu620-rgb/sample-deliver.git
cd sample-deliver
pip install -e .
```

## 使用

### 直接调用

```python
from sample_deliver import download_project_data

result = download_project_data(
    project_id="your-project-uuid",
    username="your.name",
    token="eyJ...",
    limit=10,           # 0 = 下载全部
    output_dir="./downloads",
)

print(result["report_path"])   # Excel 报告路径
print(result["downloaded"])    # 实际下载数量
print(result["tasks"])         # Task 名称列表
```

### 作为 Agent 工具使用（OpenClaw / LangChain）

```python
from langchain.tools import tool
from sample_deliver import download_project_data

download_tool = tool(download_project_data)
```

## 参数说明

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `project_id` | str | 是 | 平台项目 UUID |
| `username` | str | 是 | 平台账户名 |
| `token` | str | 是 | Bearer token |
| `limit` | int | 否 | 下载数量上限，0=全部（默认 0） |
| `output_dir` | str | 否 | 本地保存路径（默认 `./downloads`） |
| `env` | str | 否 | `prod`（默认）或 `dev` |
| `max_speed_mbps` | float | 否 | 限速 MB/s，0=不限（默认 0） |

## 返回值

```json
{
  "success": true,
  "total_available": 42,
  "downloaded": 10,
  "num_tasks": 3,
  "tasks": ["pick_apple", "pour_water", "open_drawer"],
  "report_path": "/path/to/delivery_report_20260312_153045.xlsx",
  "output_dir": "/path/to/downloads",
  "error": null
}
```

## 输出目录结构

```
downloads/
├── {task_name}/
│   ├── {episode_uuid}/          # task.zip 解压内容
│   │   └── ...
│   ├── {episode_uuid}.mcap
│   └── {episode_uuid}_vis.mcap
└── delivery_report_YYYYMMDD_HHMMSS.xlsx
```

## Excel 报告内容

- **总览** Sheet：项目 ID、下载时间、数量统计、Task 列表
- **文件清单** Sheet：每个文件的 Task Name、Episode UUID、文件路径
