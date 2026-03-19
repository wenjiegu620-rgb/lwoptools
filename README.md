# openclaw-skills

机器人数据运营工具集，包含自动化脚本和 openclaw skill。

## 目录结构

```
├── scripts/
│   ├── sample_deliver/     # Sample 数据交付下载脚本
│   └── pyproject.toml
├── skills/
│   └── pipeline-monitor/   # 工作流链路监控 skill
```

## scripts/sample_deliver

从 Lightwheel 平台下载打包完成的数据，并生成 Excel 交付报告。

### 安装

```bash
pip install git+https://github.com/wenjiegu620-rgb/sample-deliver.git#subdirectory=scripts
```

或本地安装：

```bash
cd scripts && pip install -e .
```

### 使用

```python
from sample_deliver import download_project_data

result = download_project_data(
    project_id="your-project-uuid",
    username="your.name",
    token="eyJ...",
    output_dir="./downloads",
)
```

## skills/pipeline-monitor

工作流链路监控，自动检测各节点失败堆积并通过飞书报警，支持定时监控和交互查询两种模式。

### 部署

```bash
# 复制 skill 到 openclaw
cp -r skills/pipeline-monitor ~/.openclaw/skills/

# 配置（复制模板，填入真实配置）
cp skills/pipeline-monitor/config.example.json ~/.openclaw/skills/pipeline-monitor/config.json

# 安装依赖
pip install clickhouse-driver
```

详见 `skills/pipeline-monitor/SKILL.md`。
