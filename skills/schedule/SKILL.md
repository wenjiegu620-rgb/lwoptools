---
name: schedule
description: >
  LW 交付排期看板助手。当用户说"排期"、"交付排期"、"排期看板"、"排mango"、"排grape"、
  "看一下排期"、"帮我排期"、"启动排期"、"排期工具"、"schedule"时触发。
  支持启动服务、调试问题、功能增强。
tools: Bash, Edit, Read, Write, Glob, Grep
---

# LW 交付排期看板

## 文件位置

| 文件 | 说明 |
|------|------|
| `~/Desktop/排期看板/server.py` | Python Flask 后端（端口 8000） |
| `~/Desktop/排期看板/index.html` | 前端单页应用 |

访问地址：`http://localhost:8000`

---

## 启动服务

```bash
# 检查是否已在运行
lsof -ti :8000 && echo "已运行" || echo "未运行"

# 启动（后台）
python3 ~/Desktop/排期看板/server.py &
sleep 2 && curl -s http://localhost:8000 | head -3
```

---

## 架构

```
浏览器（index.html）
    ↓ fetch localhost:8000/api/...
Flask server.py
    ├─ GET /api/projects?q=xxx    → Clickhouse 模糊搜索项目（UUID去重）
    ├─ GET /api/stock?projects=.. → MySQL 查流水线存量（8个阶段）
    ├─ GET /api/history?projects=..&start=..&end=.. → MySQL 按天聚合历史产出（3阶段）
    └─ POST /api/claude           → LiteLLM 代理（流式）
```

## 数据源

- **Clickhouse** `10.23.206.206:9000` asset库 → 项目搜索
- **MySQL** `10.23.131.202:3306` asset库 → 流水线存量（需公司 VPN）

## 流水线阶段（MySQL node_name）

| 阶段 | node_name | status=3 | 字段 |
|------|-----------|---------|------|
| 采集完成 | human_case_produce | ✓ | video_seconds |
| 待质检 | human_case_inspect | 1,2 | video_seconds |
| 质检通过 | human_case_inspect | 3 | video_seconds |
| 语义标注中 | semantics_labeling | 1 | video_seconds |
| 手势标注中 | pose_labeling | 1 | video_seconds |
| 标注完成 | labeling_complete | 3 | video_seconds |
| 打包完成 | complete_job | 3（最新） | **delivery_video_seconds** |

---

## 前端功能

- **关联采集项目**：搜索框 + 多选 tag，点「拉取存量」调 `/api/stock`
- **侧边栏客户标签**：grape/mango/orange/strawberry，点击自动搜索对应项目
- **流水线配置**：4个阶段损耗率 + 周期（天），语义/手势并行
- **每日排期表**：从 TODAY 填到截止日，✓有效 / ✗超期 标识
- **填产能工具**（用户决策，非 AI）：
  - 均摊到每天：缺口 ÷ 综合良率 ÷ 有效天数
  - 前置优先：前60%天分配70%量
  - 后置优先：后60%天分配70%量
- **汇总分析**：存量可交付 + 排期新增可交付 vs 目标，显示缺口
- **历史产出**（独立页面）：自定义日期范围，Chart.js 折线图（采集完成/质检通过/标注完成），周日标灰，下方每日明细表
- **AI 分析**：风险分析 + 补救方案（只输出建议，不自动填排期）

---

## 已知 Bug 记录（已修复）

| Bug | 原因 | 修复 |
|-----|------|------|
| 所有行日期相同 | addDays 时区问题（本地时间+UTC输出） | 改用 UTC 计算 |
| 排期不到截止日 | fillDays 只填到 last=deadline-pipeline | 改为填到 deadline |
| 填产能不工作 | gap 计算含已有 planned，再添加行后 gap=0 | 用 getGapForFill()，不扣已有 planned |
| 中间日期缺失 | fillDays 从最后行往后追加 | 改为从 TODAY 全量遍历+排序 |
| 质检通过重复计算打包数据 | 存量公式未减 packaged_h | 改为 qc_passed_h - lab_ing_h - labeled_h - packaged_h |

---

## 错误处理

| 错误 | 处理 |
|------|------|
| 连接超时 | 提示检查 VPN（需公司内网） |
| 端口占用 | `lsof -ti :8000 \| xargs kill -9` 后重启 |
| Clickhouse 认证失败 | 检查 server.py CH 配置 |
| MySQL 无数据 | 确认项目 UUID 正确，检查 VPN |
