---
name: case-copy
description: >
  Human Case 跨项目复制工具。当用户说"复制 case 到某项目"、"把质检通过/不通过的数据复制过去"、
  "case copy"、"批量复制 case"、"复制数据到另一个项目"等时触发。
  即使用户只说"帮我复制一批 case"、"跑一下复制"，结合上下文涉及 case 跨项目复制的也应触发。
tools: Bash
---

# Human Case 批量复制工具

## 脚本位置

`~/.claude/skills/case-copy/scripts/tool.py`

## 功能

从源项目按质检节点（`human_case_inspect`）状态筛选 human case，批量复制到目标项目，自动重置为初始状态，并输出 Excel 报告标注每条 case 的质检状态。

- 质检**通过**：`nodeStatus=3`，取最多 N 条
- 质检**不通过**：`nodeStatus=4`，取最多 N 条
- 一次 API 调用批量复制

## 工作流

### Step 1：告知用户

说"稍等，帮你运行复制工具"，然后执行脚本。

### Step 2：运行脚本

```bash
python3 ~/.claude/skills/case-copy/scripts/tool.py
```

脚本为**完全交互式**，会依次提示输入：
1. 用户名
2. Bearer token（不回显）
3. 源项目 UUID
4. 目标项目 UUID
5. 每种状态复制条数（默认 20）
6. 输出 Excel 文件名（默认 case_copy_report.xlsx）
7. 环境 prod/dev（默认 prod）

### Step 3：返回结果

脚本运行结束后，告知用户：
- 成功复制了多少条
- Excel 报告保存路径

## 关键 API

| 接口 | 说明 |
|------|------|
| `POST /api/asset/v2/human-case/list` | 按节点状态筛选 case |
| `POST /api/asset/v2/human-case/copy-human-case` | 批量复制到目标项目 |

**复制请求体**：
```json
{
  "current_project_uuid": "<源项目UUID>",
  "target_project_uuid":  "<目标项目UUID>",
  "human_case_ids":       ["uuid1", "uuid2", ...]
}
```

## 错误处理

| 错误 | 处理 |
|------|------|
| token 无效（401/403） | 提示用户重新从平台获取 token |
| 源项目无数据 | 提示该状态下无 case，检查筛选条件 |
| 目标项目不存在 | 提示检查目标项目 UUID |
| 依赖缺失 | `pip install requests pandas openpyxl loguru` |

## 常见修改场景

- **修改筛选节点**：改脚本中 `NODE_NAME` 常量（当前为 `human_case_inspect`）
- **修改状态码**：`STATUS_SUCCESS=3`，`STATUS_FAIL=4`
- **修改 Excel 字段**：改 `build_report()` 函数中的 `rows` 构造
