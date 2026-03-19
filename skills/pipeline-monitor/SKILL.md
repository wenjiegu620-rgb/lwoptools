# pipeline-monitor

## Description

工作流链路监控助手。监控从采集上传到标注完成的全链路节点状态，自动检测失败堆积并报警。

**触发关键词**：节点异常、节点失败、堆积、链路监控、pipeline、失败报警、查节点、workflow异常、工作流卡住、哪个节点、节点堆积、pipeline monitor、自动监控

## Behavior

本 skill 有两种工作模式：

### 模式 A：定时监控（由 cron 触发）

当 prompt 包含"执行定时监控检查"时，运行监控脚本：

```bash
python3 ~/.openclaw/skills/pipeline-monitor/scripts/monitor.py
```

解析输出 JSON，如果 `has_alert` 为 true，将 `message` 发送到飞书群 `feishu_group_id`。
如果无报警，静默（不发消息）。

### 模式 B：交互查询（用户在飞书群问问题时触发）

根据用户意图解析参数，调用 query.py：

**意图解析规则**：
1. 提取项目关键词 → `--project <keyword>`
2. 提到具体节点名 → `--node <name>`
3. 说"趋势"/"近几天"/"7天" → `--mode trend`
4. 说"详情"/"具体case"/"哪些case" → `--mode detail`（需要节点名）
5. 默认 → `--mode status`

**示例**：
- "查一下 DM_sample_0223 的节点异常" → `python3 query.py --project DM_sample_0223 --mode status`
- "data_cut 节点有哪些失败case" → `python3 query.py --project <当前项目> --node data_cut --mode detail`
- "最近7天 DM_sample_0223 的节点趋势" → `python3 query.py --project DM_sample_0223 --mode trend`

运行命令后，解析输出 JSON 中的 `message` 字段，**直接输出 Markdown 内容，不做改写**。

## Output Format

直接输出 query.py / monitor.py 返回的 `message` 字段内容（Markdown 格式），不做任何二次改写或包裹。

## Scripts

- `scripts/monitor.py` — 定时监控，查询 Clickhouse，对比快照，判断报警级别
- `scripts/query.py` — 交互查询，支持 status / detail / trend 三种模式

## Config

配置文件：`config.json`
- `alert.*` — 报警阈值（observe/warn/critical 三级）
- `feishu_group_id` — 报警目标飞书群
- `node_owners` — 节点 → 飞书 user_id 映射（critical 级别 @ 负责人）
- `monitored_projects` — 监控范围（"all" 或关键词列表）
- `clickhouse` — 数据库连接信息

## Alert Logic

| 级别 | 条件 | 动作 |
|------|------|------|
| 观察 | 失败堆积 > 10 且 1h 失败率 > 15% | 写日志，不发消息 |
| 预警 | 堆积 > 30 且 1h 增速 > 20 | 飞书群发消息，不 @ |
| 报警 | 堆积 > 50 且连续 2 次检测增长 | 飞书群 @ 负责人 |

同一节点报警后 2h 内静默，避免重复打扰。
