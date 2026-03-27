---
name: delivery-tracker
description: >
  客户交付进度追踪。当用户提到任何客户（orange、grape、mango等）的交付进度、完成情况、差距、归档、新建批次，
  或者说"X完成了"、"看一下X进度"、"给X建个新批次"时触发。
  即使用户只说客户名字加一个动作词，也应触发此 skill。
tools: Bash
---

# 交付进度追踪 Agent

## 强约束

- 所有**查询类请求**（查看进度、看交付情况等），最终输出必须包含**渲染后的 PNG 报告图片**。
- 纯文本报告（Markdown/表格）仅作为补充说明，不能替代图片结果。
- 默认调用封装脚本：`scripts/report.sh <project_name>`，其内部固定流程为：`query.py → render.py → 输出 PNG 路径`。
- 所有 delivery-tracker 报告均需：
  - 仅统计交付项目配置中的环境；
  - 如关联采集项目中存在其他环境，需在进度统计前加一行说明：`（关联采集项目中还存在未配置环境：环境Axh，环境Byh...，未纳入统计）`。
  - 在图片中包含**第三部分：分析建议/风险分析**，由模型根据当前数据生成 2~4 条要点，合并到 Markdown 后再整体渲染为 PNG。

**没有配置目标的环境不纳入数据报告。**

- 没有配置目标的环境，不纳入"一、交付进度统计"表格
- 没有配置目标的环境，不纳入"二、质检统计"表格
- 没有环境分布要求的项目，所有环境都纳入统计
- 表格标题直接显示"一、交付进度统计"和"二、质检统计"，不标注"有目标的环境"

## 脚本位置

- `~/.openclaw/skills/delivery-tracker/scripts/query.py`   — 查询并输出报告
- `~/.openclaw/skills/delivery-tracker/scripts/manage.py`  — 项目配置管理

## 内网与数据库要求（必须遵守）

- 该 skill 默认使用最新 RDS：`rm-uf69cxp907m8j6k4a.mysql.rds.aliyuncs.com:3306`。
- 查询和测试优先在内网机器执行：`ssh root@139.224.244.183`。
- 数据库连接参数优先使用环境变量覆盖：`DELIVERY_DB_HOST` / `DELIVERY_DB_PORT` / `DELIVERY_DB_USER` / `DELIVERY_DB_PASSWORD` / `DELIVERY_DB_NAME`。
- 需要保证远程机上的 skill 目录与本地一致（建议同步到：`/root/.agents/skills/delivery-tracker`）。

---

## 时长计算逻辑

- **打包成功**：取 `delivery_video_seconds`（优先），回退 `video_seconds`
- **其他所有指标**（采集质检成功、标注中、标注完成、待质检等）：直接取 `video_seconds`，不做任何修正

---

## 工作流一：查询交付进度

### Step 1：后台运行查询（脚本耗时较长，必须后台执行）

查询脚本单次全量查询可能耗时 5~10 分钟，**必须**将输出重定向到文件后台运行，不要直接阻塞等待。

```bash
# 查询单个项目
OUT=/tmp/delivery_out.txt
python3 ~/.openclaw/skills/delivery-tracker/scripts/query.py --project <项目名> \
  > "$OUT" 2>/tmp/delivery_err.txt &
PID=$!
echo "后台运行中，PID=$PID，输出 → $OUT"

# 等待完成后读取
wait $PID && cat "$OUT"
```

```bash
# 查询所有活跃项目
OUT=/tmp/delivery_out.txt
python3 ~/.openclaw/skills/delivery-tracker/scripts/query.py --all \
  > "$OUT" 2>/tmp/delivery_err.txt &
PID=$!
echo "后台运行中，PID=$PID，输出 → $OUT"

wait $PID && cat "$OUT"
```

执行 Bash 工具时**设置 timeout=600000**（10分钟），不要使用默认超时。

### Step 2：读取并展示报告

脚本 stdout 输出两块 Markdown：
- **一、交付进度统计** — 8列指标表（采集质检成功、语义标注中、手势标注中、标注中、标注完成、打包成功、目标、进度）
- **二、质检统计** — 按环境的待质检时长、质检通过数、质检失败数、通过率

如有未识别环境，末尾会有 `⚠️ 待确认环境` 区块（见下方"工作流三"处理）。

**展示规则（仅适用于有环境分布要求的项目）：**
- 没有配置目标的环境，不纳入"一、交付进度统计"表格
- 没有配置目标的环境，不纳入"二、质检统计"表格
- 没有环境分布要求的项目，所有环境都纳入统计
- 表格标题不标注"有目标的环境"，直接显示"一、交付进度统计"和"二、质检统计"

### Step 3：生成建议（Claude 生成，不在脚本里）

读完报告后，根据数据生成 **2~4 条具体建议**，聚焦以下方向：

- **进度风险**：打包完成进度 < 70% 且距交付日 ≤ 7 天
- **环境占比偏差**：当前占比偏离目标区间（如家居目标 80~85%，当前 60%）
- **质检通过率异常**：某环境通过率明显低于整体（差距 > 15%），提醒排查
- **待质检堆积**：待质检时长 > 阈值，可能影响后续标注进度
- **标注瓶颈**：标注中时长远大于标注完成，提醒关注标注产能

示例：
> 1. ⚠️ 家居进度 62%，距交付还有 3 天，缺口约 152h，建议立即评估打包产能。
> 2. 办公室当前占比 12%，超出目标上限 10%，如继续采集需关注配比。
> 3. PICO+Tracker_户外 质检通过率仅 12.4%，远低于整体 76.7%，建议排查质检失败原因。

### Step 4：渲染为图片

将完整 Markdown 报告（统计表 + 质检统计 + 建议）通过 render.py 渲染成 PNG：

```bash
python3 ~/.openclaw/skills/delivery-tracker/scripts/render.py --output /tmp/delivery_report.png << 'MDEOF'
（此处粘贴完整 Markdown 报告内容）
MDEOF
```

脚本 stdout 输出图片路径，将该路径作为最终结果返回。

---

## 工作流二：新增项目

当用户说"新建项目"、"新增一个交付批次"时：

### Step 1：收集信息（逐项确认未提供的）

| 字段 | 说明 | 必须 |
|------|------|------|
| 项目名（name） | 英文唯一标识，如 `grape_3000h` | ✅ |
| 显示名（display） | 中文名称，如 `Grape 3000小时` | ✅ |
| 交付日期 | 格式 `YYYY-MM-DD` | ✅ |
| 总目标时长 | 小时数，如 `3000` | 可选 |
| 环境配置 | 名称 + 目标时长 或 占比范围 + 最少任务数 | ✅ |
| 关联项目 | UUID + 项目名列表 | ✅ |

**环境配置两种格式：**
- 固定时长目标：`{"name": "家居", "target_hours": 100}`
- 占比目标：`{"name": "家居", "duration_ratio_min": 0.80, "duration_ratio_max": 0.85, "min_task_count": 300}`

**查询可用采集项目（模糊搜索）：**
```bash
python3 ~/.openclaw/skills/delivery-tracker/scripts/manage.py search-projects --keyword <关键词>
```
输出 JSON 列表，展示给用户选择。用户可提供关键词模糊搜索（如 `grape`、`PICO`、`mango`），或不带关键词时先搜索全量再让用户筛选。

### Step 2：执行添加

```bash
python3 ~/.openclaw/skills/delivery-tracker/scripts/manage.py add \
  --name "grape_3000h" \
  --display "Grape 3000小时" \
  --delivery-date "2026-04-30" \
  --total-hours 3000 \
  --envs '[{"name":"家居","duration_ratio_min":0.80,"duration_ratio_max":0.85,"min_task_count":300},{"name":"办公室","duration_ratio_min":0.05,"duration_ratio_max":0.10,"min_task_count":50}]' \
  --query-projects '[{"id":"uuid1","name":"grape_xxx_0401"},{"id":"uuid2","name":"grape_yyy_0401"}]'
```

### Step 3：确认

运行 `manage.py list` 让用户确认配置正确。

---

## 工作流三：处理未识别环境

当报告末尾出现 `⚠️ 待确认环境` 时：

1. 向用户展示待确认的 env_key 及其时长/条数
2. 逐个询问：**归入已有环境** 还是 **新增为独立环境**？
3. 用户确认后执行：

```bash
# 归入已有环境（如 distribution_center → 超市）
python3 ~/.openclaw/skills/delivery-tracker/scripts/manage.py add-mapping \
  --key distribution_center --env 超市

# 新增为独立环境
python3 ~/.openclaw/skills/delivery-tracker/scripts/manage.py add-mapping \
  --key factory_new --env 新工厂
```

4. **重新运行 query**，确认未识别环境已消失、数据已正确纳入统计。

---

## 工作流四：归档项目

```bash
python3 ~/.openclaw/skills/delivery-tracker/scripts/manage.py archive --name orange_dji
```

---

## 查看现有项目列表

```bash
python3 ~/.openclaw/skills/delivery-tracker/scripts/manage.py list
```

---

## 错误处理

| 错误 | 处理 |
|------|------|
| 连接超时 | 排查数据库连接问题 |
| 项目未找到 | 运行 `manage.py list` 展示可用项目 |
| 未识别环境出现 | 走工作流三，不自动归类 |
| JSON 格式错误 | 提示检查 projects.json 或 --scenes 参数格式 |
| 密码未设置 | 提示设置 `DELIVERY_DB_PASSWORD` 环境变量 |
| 脚本被杀死/无输出 | 检查 /tmp/delivery_err.txt，确认是否数据库断开或磁盘满 |
