---
name: daily-report
description: >
  运营日报生成助手。当用户说"出日报"、"生成日报"、"看今天日报"、"日报数据"、
  "今天的运营数据汇总"时触发。自动从 Clickhouse + 数据平台 API 拉取数据生成日报。
  关键词：日报、运营日报、今日汇总、采集汇总、标注汇总。
tools: Bash
---

# 运营日报生成助手

## 数据源

- **Clickhouse**: `10.23.206.206:9000`，database=`asset`（需公司内网/VPN）
- **数据平台 API**: `https://assetserver.lightwheel.net/api/asset/v1`（需公司内网）
- **Token**: 存于 `~/.claude/skills/daily-report/config.json`，约 7 天过期

## 日报结构

| 板块 | 维度 | 时间范围 |
|---|---|---|
| 采集 & 质检 | 客户 × 设备类型 | 今日 |
| 标注进度 | 语义版本（v4/v5/v6）| 今日 |
| 采集供应商明细 | 供应商 | 今日 |

## 工作流

### Step 1：执行查询

告知用户"生成中…"后立即执行：

```bash
python3 ~/.claude/skills/daily-report/scripts/query.py
```

如需查看指定日期：

```bash
python3 ~/.claude/skills/daily-report/scripts/query.py --date 2026-03-17
```

通常 15~30 秒返回。

### Step 2：输出日报

**直接输出脚本返回的 Markdown，不改写数据。**

如发现以下异常，在日报末尾简短标注：
- 某客户采集完成时长为 0（可能无采集活动，或项目未打客户标签）
- 待质检时长异常偏高（可能含历史堆积，属正常）
- 标注版本出现"未知"（对应项目未配置 `labeling_lang_version`）

### Step 3：token 过期处理

若脚本输出 `ERROR: token 已过期`，提示用户：
1. 打开 https://asset.lightwheel.net
2. 打开浏览器控制台执行：`localStorage.getItem('authToken')`
3. 复制 token 后更新 `~/.claude/skills/daily-report/config.json`

---

## 指标定义

所有指标均采用**最新 workflow run 优先**（`MAX(workflow_run_id) OVER (PARTITION BY data_uuid)`）去重逻辑，避免重跑数据重复计入。

| 指标 | 计算方式 |
|---|---|
| 今日采集完成 | `human_case_inspect` 最新 run 首次 `created_at` = 今日 |
| 累计采集完成 | `human_case_inspect` 所有 case 按 data_uuid 去重后汇总（不限日期）|
| 待质检 | `human_case_inspect` 全局最新 run 当前状态为 `running/interacting` |
| 质检通过 | `human_case_inspect` 全局最新 run 最新状态 = `success`，且 success 发生在今日 |
| 今日采集人数 | `human_case_produce` 节点去重采集员数，**按客户×设备维度跨项目去重**（同一采集员采了同客户多个项目只算 1 人）|
| 人效 | 今日采集完成时长 ÷ 今日涉及采集员人数（来自 `human_case_produce` 节点）|
| 采集流入标注 | `semantics_labeling` 最新 run 首次 `created_at` = 今日，且 `pose_labeling` 也存在 |
| 语义标注完成 | `semantics_labeling` 有史以来**首次** success = 今日（重刷不计入）|
| 手势标注完成 | `pose_labeling` 有史以来**首次** success = 今日（重刷不计入）|
| 标注完成 | `labeling_complete` 有史以来**首次** success = 今日（重刷不计入）|
| 标注吞吐率 | 标注完成 ÷ 采集流入标注 |

## 过滤规则

| 标签 | 采集指标 | 标注/打包指标 |
|---|---|---|
| 无特殊标签 | ✅ | ✅ |
| `已停采` | ❌ 排除 | ✅ 保留 |
| `已废弃` / `归档` | ❌ 排除 | ❌ 排除 |
| 无客户标签 | ❌ 不统计 | ❌ 不统计 |

客户标签：`Grape` / `Orange` / `Orange二期` / `Mango` / `Mango_egodex`（→Mango-EgoDex）/ `Strawberry`（→1X）

设备类型由 `autoConfig.human_case_workflow_key` 决定：
- `wf_E5RT0Jigk62smENT` / `wf_1btO60viv624FRgD` → 腕部相机
- `wf_G5Zj9XpZo62UzZci` → EgoDex
- 其余 → Pico

---

## Setup（首次部署）

### 1. 安装依赖

```bash
pip3 install clickhouse-driver requests
```

### 2. 创建 config.json

```bash
mkdir -p ~/.claude/skills/daily-report
cat > ~/.claude/skills/daily-report/config.json << 'EOF'
{"token": "<从浏览器 localStorage.getItem('authToken') 获取>"}
EOF
```

### 3. 放置脚本

将 `scripts/query.py` 放到 `~/.claude/skills/daily-report/scripts/query.py`。

### 4. 验证

```bash
python3 ~/.claude/skills/daily-report/scripts/query.py
```

正常应在 30 秒内输出 Markdown 日报。
