# Kiwi 交付看板 — 维护手册

## 概览

Kiwi 交付看板是一个独立的 Flask 服务，用于追踪 Kiwi 项目各场景下每个采集员在每个任务变体的采集/质检进度。

- **访问地址**：http://139.224.244.183:8001
- **服务器**：139.224.244.183（root 登录）
- **服务目录**：`~/kiwi/`
- **代码仓库**：https://github.com/wenjiegu620-rgb/lwoptools/tree/main/kiwi-kanban

---

## 文件说明

| 文件 | 作用 |
|------|------|
| `kiwi_server.py` | Flask 后端，所有 API 逻辑 |
| `kiwi.html` | 前端单页应用（纯 HTML/CSS/JS，无构建步骤） |
| `.env` | 密码和数据库配置（不提交 git） |
| `.env.example` | 配置模板 |
| `kiwi_rooms.json` | 房间编号映射，由页面「配置房间」自动生成 |
| `kiwi.log` | 运行日志 |

---

## 日常操作

### 检查服务状态

```bash
ssh root@139.224.244.183
pgrep -f kiwi_server.py && echo "运行中" || echo "已停止"
```

### 查看日志

```bash
ssh root@139.224.244.183 "tail -50 ~/kiwi/kiwi.log"
```

### 重启服务

```bash
ssh root@139.224.244.183
kill $(pgrep -f kiwi_server.py) 2>/dev/null
sleep 2
cd ~/kiwi && nohup python3 kiwi_server.py >> kiwi.log 2>&1 &
```

---

## 更新代码

### 方式一：直接在服务器上编辑（小改动）

```bash
ssh root@139.224.244.183
nano ~/kiwi/kiwi_server.py   # 或 kiwi.html
# 编辑完后重启
kill $(pgrep -f kiwi_server.py) 2>/dev/null && sleep 2
cd ~/kiwi && nohup python3 kiwi_server.py >> kiwi.log 2>&1 &
```

### 方式二：本地改好后上传（推荐）

```bash
# 1. 修改本地文件
# 2. 上传到服务器
scp kiwi_server.py root@139.224.244.183:~/kiwi/kiwi_server.py
scp kiwi.html root@139.224.244.183:~/kiwi/kiwi.html

# 3. 重启
ssh root@139.224.244.183 "kill \$(pgrep -f kiwi_server.py) 2>/dev/null; sleep 2; cd ~/kiwi && nohup python3 kiwi_server.py >> kiwi.log 2>&1 &"

# 4. 验证
curl -s -o /dev/null -w '%{http_code}' http://139.224.244.183:8001/
# 应返回 302
```

---

## 常见修改场景

### 新增 Kiwi 场景（新项目 UUID）

编辑 `kiwi_server.py`，在 `KIWI_PROJECTS` 字典中添加一行：

```python
KIWI_PROJECTS = {
    'a8fbfeed-d45c-46c0-ae5d-6d11c6b84369': 'Reception',
    '2e8836f5-fe5b-4704-b38c-0d888aa11857': 'Classroom',
    '98343fef-f4d7-4627-b35d-275ca6a9d55d': 'Library',
    '1adb4be4-e8af-4056-8f18-aabe512f71c6': 'Supermarket',
    '799ee479-b998-498f-9cb3-22a72df3af5a': 'Coffee_shop',
    # 新增：
    'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx': 'NewScene',
}
```

重启服务后，页面顶部会自动出现新场景的 tab。

### 修改达标条件（默认 qc_count ≥ 5）

`kiwi_server.py` 中无需修改（达标判断在前端）。

编辑 `kiwi.html`，找到：

```js
const TARGET = 5;
```

改为目标值，上传后刷新页面即生效（无需重启服务）。

### 修改登录密码

编辑服务器上的 `~/kiwi/.env`：

```bash
ssh root@139.224.244.183 "nano ~/kiwi/.env"
# 修改 KIWI_PASSWORD=新密码
```

重启服务后生效。

### 添加标注完成 / 打包成功状态

目前这两个状态返回 0（Kiwi 工作流暂无对应节点）。

当工作流上线后，在 `kiwi_server.py` 的 SQL 查询中补充对应节点的 LEFT JOIN，并在 `result.append(...)` 中填入真实数据。

节点名参考主看板逻辑：
- 标注完成：`labeling_complete` status=3
- 打包成功：`complete_job` status=3

---

## 配置文件（~/kiwi/.env）

```env
KIWI_PASSWORD=kiwi2026          # 看板登录密码
FLASK_SECRET_KEY=kiwi-s3cr3t-2026

MYSQL_HOST=nlb-q47d4uw6iwr03nqaxh.cn-shanghai.nlb.aliyuncsslb.com
MYSQL_PORT=3306
MYSQL_USER=wenjie.gu
MYSQL_PASSWORD=wenjie.gu_wjg789
MYSQL_DB=human_case
```

> `.env` 文件不提交 git，只存在服务器上。如果服务器重建，需要重新创建此文件。

---

## 数据说明

### 数据来源

MySQL `human_case` 库，通过 NLB 地址访问（无需 VPN）。

### 核心表

| 表 | 用途 |
|----|------|
| `human_task` | 项目信息，通过 `project_uuid` 关联 Kiwi 项目 |
| `human_case_node` | 每个 case 的节点状态 |
| `human_case_tag` | case 的元数据（环境编号、任务名、采集员信息、时长） |

### 采集完成判断

```sql
human_case_node.node_name = 'human_case_produce_complete' AND node_status = 3
```

### 质检通过判断

```sql
human_case_node.node_name = 'human_case_inspect' AND node_status = 3
```

### 任务名格式

原始 `task_name`（如 `错误书籍拿取-v03`）会被解析为：
- `task_id`：`错误书籍拿取`
- `variant`：`v03`

支持 `_v` 和 `-v` 两种分隔符，版本号自动补零（v3 → v03）。

### 缓存

API 数据缓存 5 分钟（内存缓存，重启后清空）。如需立即刷新，点页面右上角「刷新」按钮，或重启服务。

---

## 故障排查

### 页面打不开

```bash
# 检查进程
ssh root@139.224.244.183 "pgrep -f kiwi_server.py || echo '进程不存在'"

# 检查端口
ssh root@139.224.244.183 "ss -tlnp | grep 8001"

# 查看启动日志
ssh root@139.224.244.183 "tail -20 ~/kiwi/kiwi.log"

# 重启
ssh root@139.224.244.183 "cd ~/kiwi && kill \$(pgrep -f kiwi_server.py) 2>/dev/null; sleep 2; nohup python3 kiwi_server.py >> kiwi.log 2>&1 &"
```

### 数据不更新

1. 点页面「刷新」按钮（清除 5 分钟缓存）
2. 如仍无数据，检查数据库连接：
   ```bash
   ssh root@139.224.244.183 "cd ~/kiwi && python3 -c \"
   from dotenv import load_dotenv; load_dotenv('.env')
   import os, pymysql
   conn = pymysql.connect(host=os.environ['MYSQL_HOST'], port=3306,
       user=os.environ['MYSQL_USER'], password=os.environ['MYSQL_PASSWORD'],
       database='human_case')
   print('DB OK')
   conn.close()
   \""
   ```

### 登录密码忘了

```bash
ssh root@139.224.244.183 "grep KIWI_PASSWORD ~/kiwi/.env"
```

---

## 依赖

```
flask
python-dotenv
pymysql
```

首次部署安装：

```bash
pip3 install flask python-dotenv pymysql
```
