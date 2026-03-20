---
name: case-copy
description: >
  Human Case 跨项目复制工具。当用户说"复制 case 到某项目"、"把质检通过/不通过的数据复制过去"、
  "case copy"、"批量复制 case"、"复制数据到另一个项目"等时触发。
  即使用户只说"帮我复制一批 case"、"跑一下复制"，结合上下文涉及 case 跨项目复制的也应触发。
tools: Bash
---

# Human Case 智能复制 Agent

## 脚本位置

- `~/.claude/skills/case-copy/scripts/query.py`  — 查询/筛选
- `~/.claude/skills/case-copy/scripts/copy.py`   — 执行复制

## 完整工作流

### Step 1：收集需求（对话）

向用户确认以下信息（未提供的逐一询问）：

| 参数 | 说明 | 是否必须 |
|------|------|---------|
| 源项目 | UUID 或名称 | ✅ |
| 目标项目 | UUID 或名称 | ✅ |
| 用户名 | Asset 平台用户名 | ✅ |
| Bearer token | 登录 token（说明不会存储） | ✅ |
| 环境 | prod / dev | 默认 prod |
| 状态 | 质检通过 / 质检不通过 / 两者都要 | ✅ |
| 数量 | 每种状态各几条 | 默认 20 |
| 场景要求 | 自然语言描述，如"家居"、"工厂" | 可选 |
| task 去重 | 每个 task 只取一条 | 可选 |
| 是否显示时长 | 需要连 MySQL | 可选 |

**项目 UUID 解析**：若用户提供名称，调用：
```bash
python3 -c "
import requests, json
token='<token>'; username='<username>'; name='<项目名>'
resp = requests.post(
    'https://assetserver.lightwheel.net/api/asset/v1/project/list',
    headers={'Authorization': f'Bearer {token}', 'Username': username, 'Content-Type': 'application/json'},
    json={'page':1,'pageSize':20,'name':name}
)
for p in (resp.json().get('data',{}).get('list') or resp.json().get('data') or []):
    print(p.get('name'), '->', p.get('uuid') or p.get('id'))
"
```

---

### Step 2：探索项目场景（若用户有场景要求）

```bash
python3 ~/.claude/skills/case-copy/scripts/query.py \
  --token "<token>" --username "<username>" \
  --project-uuid "<src_uuid>" \
  list-scenes
```

**Claude 根据用户的自然语言描述自行匹配 scene_key**（`env_type_name` 原始值，如 `home`、`office`）。
不依赖硬编码映射，新场景自动兼容。若匹配不确定，列出候选项让用户确认。

---

### Step 3：查询候选 case

```bash
python3 ~/.claude/skills/case-copy/scripts/query.py \
  --token "<token>" --username "<username>" \
  --project-uuid "<src_uuid>" \
  query \
  --scene-key <scene_key> \
  --status <3_or_4> \
  --count <n> \
  [--task-dedup] \
  [--with-duration]
```

"两者都要"时对 status=3 和 status=4 分别查询一次。

---

### Step 4：展示确认清单

将结果格式化展示：

```
找到以下符合条件的 case，请确认是否复制：

| # | Case 名称 | Task | 场景 | 质检状态 | 时长 |
|---|-----------|------|------|---------|------|
| 1 | grape_xxx_001 | 接水水杯 | home | 质检通过 | 2m34s |
...

共 20 条（总时长：48m12s）
```

**若实际结果不足用户要求的数量**，直接告知：

> 该条件下只找到 X 条（少于要求的 N 条），是否继续复制这 X 条？

- 用户选**是** → 继续 Step 5
- 用户选**否** → 询问是否调整条件（场景/状态/数量），重新执行 Step 3

---

### Step 5：执行复制

用户确认后：

```bash
python3 ~/.claude/skills/case-copy/scripts/copy.py \
  --token "<token>" --username "<username>" \
  --src "<src_uuid>" \
  --dst "<dst_uuid>" \
  --ids "uuid1,uuid2,..." \
  --env prod
```

---

### Step 6：报告结果

告知用户成功复制了多少条，从哪个项目复制到哪个项目。

---

## 关键 API

| 接口 | 说明 |
|------|------|
| `POST /api/asset/v2/human-case/list` | 按 `nodeStatus` 筛选（3=通过，4=不通过） |
| `POST /api/asset/v2/human-case/copy-human-case` | 批量复制 |
| `POST /api/asset/v1/project/list` | 按名称查项目 UUID |

## 错误处理

| 错误 | 处理 |
|------|------|
| token 无效（401/403） | 提示重新获取 token |
| 符合条件的 case 为 0 条 | 告知用户，询问是否调整条件 |
| 符合条件的 case 不足 N 条 | 告知实际数量，询问是否继续 |
| 目标项目不存在 | 提示检查 UUID |
| MySQL 连接失败 | 跳过时长，仍正常执行复制 |
| 依赖缺失 | `pip install requests pymysql` |
