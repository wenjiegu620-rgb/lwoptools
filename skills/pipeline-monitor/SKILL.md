---
name: pipeline_monitor
description: 工作流链路监控助手，检测节点失败并@负责人报警
metadata:
  openclaw:
    trigger_keywords:
      - 节点异常
      - 节点失败
      - 堆积
      - 链路监控
      - pipeline
      - 失败报警
      - 查节点
      - workflow异常
      - 工作流卡住
      - 哪个节点
      - 节点堆积
      - pipeline monitor
      - 自动监控
---

# pipeline-monitor

## Description

工作流链路监控助手。监控从采集上传到标注完成的全链路节点状态，自动检测失败并@负责人报警。

**触发关键词**：节点异常、节点失败、堆积、链路监控、pipeline、失败报警、查节点、workflow异常、工作流卡住、哪个节点、节点堆积、pipeline monitor、自动监控

## Behavior

本 skill 有两种工作模式：

### 模式 A：定时监控（由 cron 触发）

当 prompt 包含"执行定时监控检查"时，通过 SSH 在远端服务器运行监控脚本：

```bash
export SSHPASS='<YOUR_SSH_PASS>' && sshpass -e ssh -o StrictHostKeyChecking=no root@<YOUR_SERVER_IP> "python3 ~/pipeline-monitor/scripts/monitor.py"
```

解析输出 JSON，按顺序执行：

1. **`type` 为 error** → 将 `message` 发送到飞书群 `feishu_group_id`，结束
2. **`has_alert` 为 false** → 静默，不做任何操作，结束
3. **`has_alert` 为 true**：
   a. 将 `message` 发送到飞书群 `feishu_group_id`
   b. 遍历 `todos` 数组，为每一项创建飞书待办任务：
      - 负责人：`owner_id`
      - 标题：`title`
      - 备注：`notes`
      - 截止时间：`deadline_ms`（毫秒时间戳）

### 模式 B：交互查询（用户在飞书群问问题时触发）

根据用户意图解析参数，SSH 调用 query.py：

**意图解析规则**：
1. 提取项目关键词 → `--project <keyword>`
2. 提到具体节点名 → `--node <name>`
3. 说"趋势"/"近几天"/"7天" → `--mode trend`
4. 说"详情"/"具体case"/"哪些case" → `--mode detail`（需要节点名）
5. 默认 → `--mode status`

**执行命令**：
```bash
export SSHPASS='<YOUR_SSH_PASS>' && sshpass -e ssh -o StrictHostKeyChecking=no root@<YOUR_SERVER_IP> "python3 ~/pipeline-monitor/scripts/query.py --project <keyword> [--node <name>] [--mode status|detail|trend]"
```

解析输出 JSON 中的 `message` 字段，**直接输出 Markdown 内容，不做改写**。

## Output Format

直接输出 query.py / monitor.py 返回的 `message` 字段内容（Markdown 格式），不做任何二次改写或包裹。

## Scripts（远端服务器 ~/pipeline-monitor/）

- `scripts/monitor.py` — 定时监控，查询 Clickhouse，对比快照，判断是否报警
- `scripts/query.py` — 交互查询，支持 status / detail / trend 三种模式

## Config（远端服务器 ~/pipeline-monitor/config.json）

- `alert.silence_hours` — 同一节点报警后静默时长（默认 2h）
- `alert.growth_threshold` — 触发增长报警的阈值（默认 20）
- `alert.volume_threshold` — 触发总量报警的阈值（默认 50）
- `alert.todo_deadline_hours` — 待办截止时长（默认 2h）
- `feishu_group_id` — 报警目标飞书群
- `node_owners` — 节点 → 飞书 user_id 映射（报警时 @负责人并创建待办）
- `node_names` — 节点英文名 → 中文名映射
- `monitored_projects` — 监控范围（"all" 或项目关键词列表）
- `clickhouse` — 数据库连接信息
