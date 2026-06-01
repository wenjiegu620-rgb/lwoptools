# Kiwi 交付看板

独立部署的 Kiwi 项目交付进度看板。

## 文件说明

| 文件 | 说明 |
|------|------|
| `kiwi_server.py` | Flask 后端，端口 8001 |
| `kiwi.html` | 前端页面 |
| `.env` | 环境变量（密码等，不提交 git） |
| `kiwi_rooms.json` | 房间编号映射（自动生成） |

## 启动

```bash
# 安装依赖（首次）
pip3 install flask python-dotenv pymysql

# 启动（前台）
python3 kiwi_server.py

# 启动（后台）
nohup python3 kiwi_server.py >> kiwi.log 2>&1 &

# 停止
kill $(pgrep -f kiwi_server.py)
```

访问：http://服务器IP:8001

## 更新部署

```bash
git pull
kill $(pgrep -f kiwi_server.py)
nohup python3 kiwi_server.py >> kiwi.log 2>&1 &
```

## 配置

复制 `.env.example` 为 `.env`，填入实际密码：

```bash
cp .env.example .env
# 编辑 .env，修改 KIWI_PASSWORD 和 MYSQL_PASSWORD
```

## 项目配置

如需修改 Kiwi 项目 UUID，编辑 `kiwi_server.py` 中的 `KIWI_PROJECTS` 字典。
