#!/usr/bin/env python3
"""
Kiwi 交付看板 独立服务
运行: python3 kiwi_server.py
访问: http://localhost:8001
"""
import json, os, re, time
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

from flask import Flask, request, jsonify, send_from_directory, session, redirect
import pymysql

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "kiwi-s3cr3t-2026")

# ─── 内存缓存 ─────────────────────────────────────────────────
_cache = {}

def _cache_get(key):
    entry = _cache.get(key)
    if entry and entry[1] > time.time():
        return entry[0]
    return None

def _cache_set(key, value, ttl):
    _cache[key] = (value, time.time() + ttl)

# ─── MySQL 连接 ───────────────────────────────────────────────
def mysql_conn():
    return pymysql.connect(
        host=os.environ.get("MYSQL_HOST", "nlb-q47d4uw6iwr03nqaxh.cn-shanghai.nlb.aliyuncsslb.com"),
        port=int(os.environ.get("MYSQL_PORT", 3306)),
        user=os.environ.get("MYSQL_USER", "wenjie.gu"),
        password=os.environ["MYSQL_PASSWORD"],
        database=os.environ.get("MYSQL_DB", "human_case"),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=30,
        read_timeout=300,
        write_timeout=300,
    )

# ─── Kiwi 项目配置 ────────────────────────────────────────────
KIWI_PROJECTS = {
    'a8fbfeed-d45c-46c0-ae5d-6d11c6b84369': 'Reception',
    '2e8836f5-fe5b-4704-b38c-0d888aa11857': 'Classroom',
    '98343fef-f4d7-4627-b35d-275ca6a9d55d': 'Library',
    '1adb4be4-e8af-4056-8f18-aabe512f71c6': 'Supermarket',
    '799ee479-b998-498f-9cb3-22a72df3af5a': 'Coffee_shop',
}

ROOMS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'kiwi_rooms.json')

def _load_rooms():
    if os.path.exists(ROOMS_FILE):
        with open(ROOMS_FILE, encoding='utf-8') as f:
            return json.load(f)
    return {}

def _save_rooms(data):
    with open(ROOMS_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# ─── 鉴权 ─────────────────────────────────────────────────────
KIWI_PASSWORD = os.environ.get("KIWI_PASSWORD", "kiwi2026")

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        pwd = request.form.get('password', '')
        if pwd == KIWI_PASSWORD:
            session['authed'] = True
            return redirect(request.args.get('next', '/'))
        return login_page(error=True)
    return login_page()

def login_page(error=False):
    err_html = '<p style="color:#f87171;margin-top:8px">密码错误</p>' if error else ''
    return f'''<!DOCTYPE html><html lang="zh"><head>
<meta charset="UTF-8"><title>Kiwi 看板登录</title>
<style>*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:#0a0a1a;display:flex;align-items:center;justify-content:center;height:100vh;font-family:system-ui}}
.box{{background:#13132d;border:1px solid rgba(99,102,241,.3);border-radius:12px;padding:32px;width:320px}}
h2{{color:#c7d2fe;margin-bottom:20px;font-size:16px}}
input{{width:100%;padding:8px 12px;background:#1e293b;border:1px solid #334155;color:#e2e8f0;border-radius:6px;font-size:14px;outline:none}}
input:focus{{border-color:#6366f1}}
button{{margin-top:12px;width:100%;padding:9px;background:#4f46e5;color:#fff;border:none;border-radius:6px;cursor:pointer;font-size:14px}}
button:hover{{background:#4338ca}}</style></head>
<body><div class="box"><h2>Kiwi 交付看板</h2>
<form method="post"><input type="password" name="password" placeholder="请输入密码" autofocus>
{err_html}<button type="submit">登录</button></form></div></body></html>'''

@app.route('/logout')
def logout():
    session.clear()
    return redirect('/login')

# ─── 页面 ─────────────────────────────────────────────────────
@app.route('/')
def index():
    if not session.get('authed'):
        return redirect('/login')
    return send_from_directory('.', 'kiwi.html')

# ─── API: 房间配置 ─────────────────────────────────────────────
@app.route('/api/rooms', methods=['GET'])
def rooms_get():
    if not session.get('authed'):
        return jsonify({'error': 'unauthorized'}), 401
    return jsonify(_load_rooms())

@app.route('/api/rooms', methods=['POST'])
def rooms_post():
    if not session.get('authed'):
        return jsonify({'error': 'unauthorized'}), 401
    _save_rooms(request.get_json())
    return jsonify({'ok': True})

# ─── API: 进度数据 ─────────────────────────────────────────────
@app.route('/api/progress')
def progress():
    if not session.get('authed'):
        return jsonify({'error': 'unauthorized'}), 401

    scene_filter = request.args.get('scene', '')
    uuids = [u for u, s in KIWI_PROJECTS.items() if not scene_filter or s == scene_filter]
    if not uuids:
        return jsonify([])

    cache_key = f'progress:{scene_filter}'
    cached = _cache_get(cache_key)
    if cached is not None:
        return jsonify(cached)

    placeholders = ','.join(['%s'] * len(uuids))
    conn = mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT
                    ht.project_uuid,
                    JSON_UNQUOTE(JSON_EXTRACT(ctx.value,'$.env_num'))   AS env_num,
                    JSON_UNQUOTE(JSON_EXTRACT(ctx.value,'$.task_name')) AS full_task_name,
                    JSON_UNQUOTE(JSON_EXTRACT(prod.value,'$.producer.producer_name')) AS producer,
                    JSON_UNQUOTE(JSON_EXTRACT(prod.value,'$.producer.producer_group')) AS producer_group,
                    COUNT(*) AS collected_count,
                    SUM(IFNULL(CAST(JSON_EXTRACT(prod.value,'$.data_info.duration') AS DECIMAL(10,2)),0)) AS collected_dur,
                    SUM(CASE WHEN qc.human_case_id IS NOT NULL THEN 1 ELSE 0 END) AS qc_count,
                    SUM(CASE WHEN qc.human_case_id IS NOT NULL
                             THEN IFNULL(CAST(JSON_EXTRACT(prod.value,'$.data_info.duration') AS DECIMAL(10,2)),0)
                             ELSE 0 END) AS qc_dur
                FROM human_case_node hcn
                JOIN human_task ht   ON ht.uuid  = hcn.task_uuid
                JOIN human_case_tag ctx  ON ctx.human_case_id  = hcn.human_case_id AND ctx.type  = 'context_tags'
                JOIN human_case_tag prod ON prod.human_case_id = hcn.human_case_id AND prod.type = 'produce_tags'
                LEFT JOIN (
                    SELECT DISTINCT human_case_id FROM human_case_node
                    WHERE node_name='human_case_inspect' AND node_status=3
                ) qc ON qc.human_case_id = hcn.human_case_id
                WHERE ht.project_uuid IN ({placeholders})
                  AND hcn.node_name = 'human_case_produce_complete'
                  AND hcn.node_status = 3
                GROUP BY ht.project_uuid, env_num, full_task_name, producer, producer_group
            """, uuids)
            rows = cur.fetchall()
    finally:
        conn.close()

    result = []
    for r in rows:
        full_task = r['full_task_name'] or ''
        m = re.match(r'^(.+)[_-][vV](\d+)$', full_task)
        if m:
            task_id = m.group(1)
            variant = 'v' + m.group(2).zfill(2)
        else:
            task_id = full_task
            variant = 'v01'
        result.append({
            'scene_type':      KIWI_PROJECTS.get(r['project_uuid'], ''),
            'env_num':         r['env_num'] or '',
            'task_id':         task_id,
            'variant':         variant,
            'producer':        r['producer'] or '',
            'producer_group':  r['producer_group'] or '',
            'collected_count': int(r['collected_count'] or 0),
            'collected_dur':   round(float(r['collected_dur'] or 0), 1),
            'qc_count':        int(r['qc_count'] or 0),
            'qc_dur':          round(float(r['qc_dur'] or 0), 1),
            'labeled_count':   0,
            'labeled_dur':     0.0,
            'packaged_count':  0,
            'packaged_dur':    0.0,
        })

    _cache_set(cache_key, result, 300)
    return jsonify(result)


if __name__ == '__main__':
    print("Kiwi 看板已启动 → http://localhost:8001")
    app.run(host='0.0.0.0', port=8001, debug=False, threaded=True)
