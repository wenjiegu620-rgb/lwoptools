---
name: kiwi-kanban
description: >
  Kiwi 交付看板助手。当用户说"kiwi"、"kiwi看板"、"kiwi交付"、"kiwi挂了"、
  "重启kiwi"、"kiwi进度"、"kiwi项目" 时触发。
  支持启动/停止/重启 Kiwi 独立服务、调试问题、功能增强。
tools: Bash, Edit, Read, Write, Glob, Grep
---

# Kiwi 交付看板

## 服务信息

| 项目 | 值 |
|------|-----|
| 远端服务器 | `139.224.244.183` |
| 用户 | `root` |
| 密码 | `Lightwheel*2026` |
| 文件目录 | `~/kiwi/` |
| 端口 | `8001` |
| 访问地址 | `http://139.224.244.183:8001` |
| GitHub | `https://github.com/wenjiegu620-rgb/lwoptools/tree/main/kiwi-kanban` |

## 本地文件

| 文件 | 说明 |
|------|------|
| `/Users/glzn/lwoptools/kiwi-kanban/kiwi_server.py` | Flask 后端（~170行，独立） |
| `/Users/glzn/lwoptools/kiwi-kanban/kiwi.html` | 前端单页应用 |
| `/Users/glzn/lwoptools/kiwi-kanban/.env.example` | 环境变量模板 |

---

## SSH 连接方式

```bash
export SSHPASS='Lightwheel*2026'
sshpass -e ssh -o StrictHostKeyChecking=no root@139.224.244.183 "命令"

# 上传文件
sshpass -e scp -o StrictHostKeyChecking=no 本地文件 root@139.224.244.183:~/kiwi/文件名
```

---

## 启动 / 停止 / 重启

```bash
export SSHPASS='Lightwheel*2026'

# 检查是否在跑
sshpass -e ssh -o StrictHostKeyChecking=no root@139.224.244.183 \
  "pgrep -f kiwi_server.py && echo running || echo stopped"

# 启动
sshpass -e ssh -o StrictHostKeyChecking=no root@139.224.244.183 \
  "cd /root/kiwi && nohup python3 kiwi_server.py >> kiwi.log 2>&1 &"

# 重启（分步执行）
sshpass -e ssh -o StrictHostKeyChecking=no root@139.224.244.183 "kill \$(pgrep -f kiwi_server.py) 2>/dev/null; true"
sleep 2
sshpass -e ssh -o StrictHostKeyChecking=no root@139.224.244.183 \
  "cd /root/kiwi && nohup python3 kiwi_server.py >> kiwi.log 2>&1 &"

# 查看日志
sshpass -e ssh -o StrictHostKeyChecking=no root@139.224.244.183 \
  "tail -30 ~/kiwi/kiwi.log"
```

---

## 部署流程

1. 修改本地 `/Users/glzn/lwoptools/kiwi-kanban/` 下的文件
2. SCP 上传到服务器 `~/kiwi/`
3. 重启 kiwi_server.py
4. 验证：`curl -s -o /dev/null -w '%{http_code}' http://139.224.244.183:8001/`（应为 302）
5. `git push origin main` 同步到 GitHub

---

## 架构

```
浏览器（kiwi.html）
    ↓ fetch 139.224.244.183:8001/api/...
Flask kiwi_server.py（端口 8001，独立进程）
    ├─ GET/POST /login          → 独立登录（KIWI_PASSWORD）
    ├─ GET /logout              → 退出
    ├─ GET /                    → kiwi.html 页面
    ├─ GET /api/rooms           → 读取房间映射（kiwi_rooms.json）
    ├─ POST /api/rooms          → 保存房间映射
    └─ GET /api/progress        → 查询采集/质检进度（MySQL human_case 库）
```

## 数据源

- **MySQL** `nlb-q47d4uw6iwr03nqaxh.cn-shanghai.nlb.aliyuncsslb.com:3306`
  - database: `human_case`
  - user: `wenjie.gu` / password: `wenjie.gu_wjg789`

---

## 鉴权配置（~/kiwi/.env）

```env
KIWI_PASSWORD=kiwi2026          # Kiwi 看板登录密码
FLASK_SECRET_KEY=kiwi-s3cr3t-2026

MYSQL_HOST=nlb-q47d4uw6iwr03nqaxh.cn-shanghai.nlb.aliyuncsslb.com
MYSQL_PORT=3306
MYSQL_USER=wenjie.gu
MYSQL_PASSWORD=wenjie.gu_wjg789
MYSQL_DB=human_case
```

---

## Kiwi 项目配置（kiwi_server.py 中的 KIWI_PROJECTS）

```python
KIWI_PROJECTS = {
    'a8fbfeed-d45c-46c0-ae5d-6d11c6b84369': 'Reception',
    '2e8836f5-fe5b-4704-b38c-0d888aa11857': 'Classroom',
    '98343fef-f4d7-4627-b35d-275ca6a9d55d': 'Library',
    '1adb4be4-e8af-4056-8f18-aabe512f71c6': 'Supermarket',
    '799ee479-b998-498f-9cb3-22a72df3af5a': 'Coffee_shop',
}
```

新增场景：在此字典加一行 `'<project_uuid>': '<scene_name>'`，重启服务即生效。

---

## 数据逻辑

### 采集完成
- `human_case_node.node_name = 'human_case_produce_complete'` AND `node_status = 3`

### 质检通过
- LEFT JOIN 子查询：`human_case_node.node_name = 'human_case_inspect'` AND `node_status = 3`

### 达标判断
- 每个采集员在每个变体的 `qc_count >= 5` 即达标（TARGET = 5）

### 任务名解析
- `full_task_name` 格式：`错误书籍拿取-v03`
- 正则：`^(.+)[_-][vV](\d+)$` → task_id + variant（如 v03）

### 房间映射
- `env_num`（如 `office_x_corporate_office_0119`）→ 用户配置的 Room_01 等
- 存储在 `~/kiwi/kiwi_rooms.json`，通过「配置房间」弹窗设置

---

## 与主看板的关系

- 主看板（端口 8000）的 `/kiwi` 路由已改为 302 跳转到 `http://139.224.244.183:8001/`
- 两个服务完全独立，互不影响
- Kiwi 服务崩溃不影响主看板

---

## 已知问题 & 修复记录

| 问题 | 原因 | 修复 |
|------|------|------|
| 标注完成/打包成功显示 0 | Kiwi 工作流暂无这两个节点 | 返回 0 占位，后续节点上线后补充 |
