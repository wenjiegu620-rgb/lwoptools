#!/usr/bin/env python3
"""
排期看板 本地服务器
运行: python3 server.py
访问: http://localhost:8000
"""
import json, os, re, glob, subprocess, uuid as _uuid, time
from collections import defaultdict
from dotenv import load_dotenv
import openpyxl
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from datetime import date, timedelta
from flask import Flask, request, jsonify, Response, send_from_directory, session, redirect
from clickhouse_driver import Client
import pymysql
import urllib.request, urllib.error, ssl

# ─── UUID 校验 ────────────────────────────────────────────────
UUID_RE = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I)

# ─── 内存缓存 ─────────────────────────────────────────────────
_cache = {}  # key -> (value, expire_ts)

def _cache_get(key):
    entry = _cache.get(key)
    if entry and entry[1] > time.time():
        return entry[0]
    return None

def _cache_set(key, value, ttl):
    _cache[key] = (value, time.time() + ttl)

def safe_uuids(ids):
    return [i for i in ids if UUID_RE.match(i)]

# ─── Clickhouse（项目搜索） ───────────────────────────────────
CH = dict(host=os.environ["CH_HOST"], port=int(os.environ.get("CH_PORT", 9000)),
          user=os.environ["CH_USER"], password=os.environ["CH_PASSWORD"], database="asset")

def ck():
    return Client(**CH)

# ─── MySQL（存量查询，与 delivery tracker 完全一致） ──────────
MYSQL = dict(
    host=os.environ["MYSQL_HOST"], port=int(os.environ.get("MYSQL_PORT", 3306)),
    user=os.environ["MYSQL_USER"], password=os.environ["MYSQL_PASSWORD"],
    database="asset", charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor,
    connect_timeout=30,
)

def mysql():
    return pymysql.connect(**MYSQL)

# ─── LiteLLM ─────────────────────────────────────────────────
LITELLM_URL = os.environ.get("LITELLM_URL", "https://ai.lightwheel.net:8086/v1/chat/completions")
LITELLM_KEY = os.environ["LITELLM_KEY"]

KANBAN_PASSWORD = os.environ.get("KANBAN_PASSWORD", "")
API_KEY         = os.environ.get("API_KEY", "")
# 供应商密码 map：{ 组名 -> 密码 }
VENDOR_PASSWORDS = {k[7:]: v for k, v in os.environ.items() if k.startswith("VENDOR_") and v}

app = Flask(__name__, static_folder=".", static_url_path="")
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "lw-kanban-secret")
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"

_LOGIN_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>LW 运营看板</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,"PingFang SC","Microsoft YaHei",sans-serif;
     background:#f0f2f5;display:flex;align-items:center;justify-content:center;min-height:100vh}
.card{background:#fff;border-radius:16px;padding:40px 36px;width:360px;
      box-shadow:0 4px 24px rgba(0,0,0,.10)}
h1{font-size:20px;font-weight:700;color:#1a1a2e;margin-bottom:6px}
p{font-size:13px;color:#999;margin-bottom:28px}
label{font-size:13px;color:#555;display:block;margin-bottom:6px}
input[type=password]{width:100%;padding:10px 14px;border:1.5px solid #e2e8f0;border-radius:8px;
  font-size:14px;outline:none;transition:border .2s}
input[type=password]:focus{border-color:#6366f1}
button{width:100%;margin-top:18px;padding:11px;background:#6366f1;color:#fff;border:none;
  border-radius:8px;font-size:14px;font-weight:600;cursor:pointer;transition:background .2s}
button:hover{background:#4f46e5}
.err{margin-top:14px;font-size:13px;color:#ef4444;text-align:center}
</style>
</head>
<body>
<div class="card">
  <h1>🌐 LW 运营看板</h1>
  <p>请输入访问密码</p>
  <form method="POST" action="/login">
    <label>密码</label>
    <input type="password" name="password" autofocus autocomplete="current-password">
    <button type="submit">进入</button>
  </form>
  <div class="err">__ERROR__</div>
</div>
</body>
</html>"""


@app.before_request
def _check_auth():
    # 供应商路径走独立鉴权
    if request.path.startswith("/vendor") or request.path.startswith("/api/vendor"):
        return None
    if request.path in ("/login", "/logout"):
        return None
    if not KANBAN_PASSWORD:          # 未设置密码则不拦截
        return None
    if not session.get("authed"):
        return redirect("/login")


@app.route("/login", methods=["GET", "POST"])
def login():
    error = ""
    if request.method == "POST":
        if request.form.get("password", "") == KANBAN_PASSWORD:
            session["authed"] = True
            return redirect("/")
        error = "密码错误，请重试"
    return _LOGIN_HTML.replace("__ERROR__", error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")


# ═══════════════════════════════════════════════════════════════
#  供应商登录 & 绩效页
# ═══════════════════════════════════════════════════════════════
_VENDOR_LOGIN_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>供应商看板</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,"PingFang SC","Microsoft YaHei",sans-serif;
     background:#0f172a;display:flex;align-items:center;justify-content:center;min-height:100vh}
.card{background:#1e293b;border-radius:16px;padding:40px 36px;width:380px;
      box-shadow:0 4px 24px rgba(0,0,0,.4)}
h1{font-size:20px;font-weight:700;color:#e2e8f0;margin-bottom:6px}
p{font-size:13px;color:#64748b;margin-bottom:28px}
label{font-size:13px;color:#94a3b8;display:block;margin-bottom:6px}
input{width:100%;padding:10px 14px;border:1.5px solid #334155;border-radius:8px;
  background:#0f172a;color:#e2e8f0;font-size:14px;outline:none;transition:border .2s;margin-bottom:16px}
input:focus{border-color:#3b82f6}
button{width:100%;margin-top:4px;padding:11px;background:#3b82f6;color:#fff;border:none;
  border-radius:8px;font-size:14px;font-weight:600;cursor:pointer;transition:background .2s}
button:hover{background:#2563eb}
.err{margin-top:14px;font-size:13px;color:#f87171;text-align:center}
</style>
</head>
<body>
<div class="card">
  <h1>供应商看板</h1>
  <p>请输入您的供应商名称和密码</p>
  <form method="POST" action="/vendor/login">
    <label>供应商名称</label>
    <input type="text" name="group" placeholder="如：聚航" autofocus autocomplete="username" value="__GROUP__">
    <label>密码</label>
    <input type="password" name="password" autocomplete="current-password">
    <button type="submit">进入</button>
  </form>
  <div class="err">__ERROR__</div>
</div>
</body>
</html>"""


@app.route("/vendor")
@app.route("/vendor/")
def vendor_index():
    if session.get("vendor_group"):
        return redirect("/vendor/performance")
    return _VENDOR_LOGIN_HTML.replace("__ERROR__", "").replace("__GROUP__", "")


@app.route("/vendor/login", methods=["POST"])
def vendor_login():
    group = request.form.get("group", "").strip()
    password = request.form.get("password", "")
    expected = VENDOR_PASSWORDS.get(group, "")
    if expected and password == expected:
        session["vendor_group"] = group
        return redirect("/vendor/performance")
    error = "供应商名称或密码错误"
    return _VENDOR_LOGIN_HTML.replace("__ERROR__", error).replace("__GROUP__", group)


@app.route("/vendor/logout")
def vendor_logout():
    session.pop("vendor_group", None)
    return redirect("/vendor")


@app.route("/vendor/performance")
def vendor_performance():
    if not session.get("vendor_group"):
        return redirect("/vendor")
    return send_from_directory(".", "vendor_performance.html")


@app.route("/api/vendor/collectors")
def vendor_collectors():
    """供应商专属采集详情（按天，只返回自己组）"""
    group = session.get("vendor_group")
    if not group:
        return jsonify({"error": "unauthorized"}), 401

    date_str = request.args.get("date", str(date.today()))

    # 当日数据缓存 5 分钟，累计/待质检数据缓存 1 小时
    daily_cache_key = f"vendor_daily:{group}:{date_str}"
    cumul_cache_key = f"vendor_cumul:{group}"
    cached_daily = _cache_get(daily_cache_key)
    cached_cumul = _cache_get(cumul_cache_key)

    conn = mysql()
    try:
        with conn.cursor() as cur:
            if cached_daily:
                produce_rows, qc_map = cached_daily
            else:
                cur.execute("""
                    SELECT hc.producer, hc.produced_by_group AS vendor,
                           hcn.node_created_at AS t_start, hcn.node_updated_at AS t_end,
                           IFNULL(hc.video_seconds, 0) AS vsec
                    FROM human_cases hc
                    JOIN human_case_nodes hcn ON hcn.human_case_id = hc.id
                    WHERE hcn.node_name = 'human_case_produce_complete'
                      AND hcn.node_status = 3
                      AND DATE(hcn.node_updated_at) = %s
                      AND hc.deleted_at IS NULL
                      AND hc.produced_by_group = %s
                    ORDER BY hc.producer, hcn.node_created_at
                """, [date_str, group])
                produce_rows = cur.fetchall()

                cur.execute("""
                    SELECT hc.producer,
                        COUNT(*) AS qc_total_cases,
                        SUM(CASE WHEN q.passed = 1 THEN 1 ELSE 0 END) AS qc_passed_cases,
                        SUM(CASE WHEN q.passed = 1 THEN IFNULL(hc.video_seconds, 0) ELSE 0 END) / 3600.0 AS qc_h
                    FROM human_cases hc
                    JOIN (
                        SELECT hcn.human_case_id, CASE WHEN hcn.node_status = 3 THEN 1 ELSE 0 END AS passed
                        FROM human_case_nodes hcn
                        WHERE hcn.node_name = 'human_case_sampling' AND hcn.node_status IN (3, 4)
                          AND DATE(hcn.node_updated_at) = %s
                        UNION ALL
                        SELECT hcn.human_case_id, CASE WHEN hcn.node_status = 3 THEN 1 ELSE 0 END AS passed
                        FROM human_case_nodes hcn
                        WHERE hcn.node_name = 'human_case_inspect' AND hcn.node_status IN (3, 4)
                          AND DATE(hcn.node_updated_at) = %s
                          AND hcn.human_case_id NOT IN (
                              SELECT DISTINCT human_case_id FROM human_case_nodes WHERE node_name = 'human_case_sampling'
                          )
                    ) q ON q.human_case_id = hc.id
                    WHERE hc.deleted_at IS NULL AND hc.produced_by_group = %s
                    GROUP BY hc.producer
                """, [date_str, date_str, group])
                qc_map = {r["producer"]: {"qc_cases": int(r["qc_passed_cases"] or 0),
                                           "qc_total": int(r["qc_total_cases"] or 0),
                                           "qc_h": float(r["qc_h"] or 0)} for r in cur.fetchall()}
                _cache_set(daily_cache_key, (produce_rows, qc_map), 300)  # 5 分钟

            if cached_cumul:
                total_collect_map, total_qc_map, pending_qc_map = cached_cumul
            else:
                cur.execute("""
                    SELECT hc.producer,
                        SUM(IFNULL(hc.video_seconds, 0)) / 3600.0 AS total_collect_h
                    FROM human_cases hc
                    JOIN human_case_nodes hcn ON hcn.human_case_id = hc.id
                    WHERE hcn.node_name = 'human_case_produce_complete'
                      AND hcn.node_status = 3
                      AND hc.deleted_at IS NULL
                      AND hc.produced_by_group = %s
                    GROUP BY hc.producer
                """, [group])
                total_collect_map = {r["producer"]: float(r["total_collect_h"] or 0) for r in cur.fetchall()}

                cur.execute("""
                    SELECT hc.producer,
                        COUNT(*) AS total_qc_total,
                        SUM(CASE WHEN q.passed = 1 THEN 1 ELSE 0 END) AS total_qc_passed,
                        SUM(CASE WHEN q.passed = 1 THEN IFNULL(hc.video_seconds, 0) ELSE 0 END) / 3600.0 AS total_qc_h
                    FROM human_cases hc
                    JOIN (
                        SELECT hcn.human_case_id, CASE WHEN hcn.node_status = 3 THEN 1 ELSE 0 END AS passed
                        FROM human_case_nodes hcn
                        WHERE hcn.node_name = 'human_case_sampling' AND hcn.node_status IN (3, 4)
                        UNION ALL
                        SELECT hcn.human_case_id, CASE WHEN hcn.node_status = 3 THEN 1 ELSE 0 END AS passed
                        FROM human_case_nodes hcn
                        WHERE hcn.node_name = 'human_case_inspect' AND hcn.node_status IN (3, 4)
                          AND hcn.human_case_id NOT IN (
                              SELECT DISTINCT human_case_id FROM human_case_nodes WHERE node_name = 'human_case_sampling'
                          )
                    ) q ON q.human_case_id = hc.id
                    WHERE hc.deleted_at IS NULL AND hc.produced_by_group = %s
                    GROUP BY hc.producer
                """, [group])
                total_qc_map = {r["producer"]: {
                    "total_qc_h": float(r["total_qc_h"] or 0),
                    "total_qc_passed": int(r["total_qc_passed"] or 0),
                    "total_qc_total": int(r["total_qc_total"] or 0),
                } for r in cur.fetchall()}

                cur.execute("""
                    SELECT hc.producer,
                        SUM(IFNULL(hc.video_seconds, 0)) / 3600.0 AS pending_qc_h
                    FROM human_cases hc
                    JOIN (
                        SELECT hcn.human_case_id FROM human_case_nodes hcn
                        WHERE hcn.node_name = 'human_case_sampling' AND hcn.node_status IN (1, 2)
                        UNION ALL
                        SELECT hcn.human_case_id FROM human_case_nodes hcn
                        WHERE hcn.node_name = 'human_case_inspect' AND hcn.node_status IN (1, 2)
                          AND hcn.human_case_id NOT IN (
                              SELECT DISTINCT human_case_id FROM human_case_nodes WHERE node_name = 'human_case_sampling'
                          )
                    ) pending ON pending.human_case_id = hc.id
                    WHERE hc.deleted_at IS NULL AND hc.produced_by_group = %s
                    GROUP BY hc.producer
                """, [group])
                pending_qc_map = {r["producer"]: float(r["pending_qc_h"] or 0) for r in cur.fetchall()}
                _cache_set(cumul_cache_key, (total_collect_map, total_qc_map, pending_qc_map), 3600)  # 1 小时

    finally:
        conn.close()

    sessions_by_p = defaultdict(list)
    vsec_by_p = defaultdict(float)
    cases_by_p = defaultdict(int)

    for row in produce_rows:
        p = row["producer"]
        sessions_by_p[p].append((row["t_start"], row["t_end"]))
        vsec_by_p[p] += float(row["vsec"])
        cases_by_p[p] += 1

    GAP = 30 * 60
    persons = []
    for p, segs in sessions_by_p.items():
        points = sorted(set(e for _, e in segs))
        if not points:
            continue
        online_sec = 0
        seg_start = seg_end = points[0]
        for pt in points[1:]:
            if (pt - seg_end).total_seconds() <= GAP:
                seg_end = pt
            else:
                online_sec += (seg_end - seg_start).total_seconds()
                seg_start = seg_end = pt
        online_sec += (seg_end - seg_start).total_seconds()

        qc = qc_map.get(p, {})
        qc_pass = int(qc.get("qc_cases") or 0)
        qc_tot = int(qc.get("qc_total") or 0)
        tqc = total_qc_map.get(p, {})
        tqc_passed = int(tqc.get("total_qc_passed") or 0)
        tqc_total = int(tqc.get("total_qc_total") or 0)
        persons.append({
            "producer":        p,
            # 累计
            "total_collect_h": round(total_collect_map.get(p, 0), 1),
            "total_qc_h":      round(tqc.get("total_qc_h") or 0, 1),
            "total_qc_rate":   round(tqc_passed / tqc_total * 100, 1) if tqc_total > 0 else 0,
            # 当日
            "collect_cases":   cases_by_p[p],
            "collect_h":       round(vsec_by_p[p] / 3600, 2),
            "qc_cases":        qc_pass,
            "qc_total":        qc_tot,
            "qc_h":            round(float(qc.get("qc_h") or 0), 2),
            "qc_rate":         round(qc_pass / qc_tot * 100, 1) if qc_tot > 0 else 0,
            # 待质检
            "pending_qc_h":    round(pending_qc_map.get(p, 0), 1),
            # 在线
            "online_h":        round(online_sec / 3600, 2),
            "first_seen":      segs[0][0].strftime("%H:%M"),
            "last_seen":       segs[-1][1].strftime("%H:%M"),
        })
    persons.sort(key=lambda x: -x["collect_h"])

    tc = sum(p["collect_h"] for p in persons)
    tq = sum(p["qc_h"] for p in persons)
    tqpass = sum(p["qc_cases"] for p in persons)
    tqtotal = sum(p["qc_total"] for p in persons)
    return jsonify({
        "date": date_str,
        "group": group,
        "total_persons": len(persons),
        "total_collect_h": round(tc, 2),
        "total_qc_h": round(tq, 2),
        "overall_qc_rate": round(tqpass / tqtotal * 100, 1) if tqtotal > 0 else 0,
        "persons": persons,
    })


@app.route("/api/vendor/collector-stats")
def vendor_collector_stats():
    """供应商专属：只返回自己组的数据"""
    group = session.get("vendor_group")
    if not group:
        return jsonify({"error": "unauthorized"}), 401

    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")

    # 复用主接口逻辑，强制 group 为 session 中的值
    from flask import current_app
    with current_app.test_request_context(
        f"/api/collector-stats?start_date={start_date}&end_date={end_date}&group={group}"
    ):
        # 直接调用底层查询函数
        pass

    # 直接内联查询（避免 request context 问题）
    cache_key = f"vendor_stats:{group}:{start_date}:{end_date}"
    cached = _cache_get(cache_key)
    if cached:
        return jsonify(cached)

    conn = mysql()
    try:
        with conn.cursor() as cur:
            sql_collect = """
                SELECT hc.producer, hc.produced_by_group AS grp,
                       COUNT(DISTINCT hc.id) AS total_cases
                FROM human_cases hc
                JOIN human_case_nodes hcn ON hcn.human_case_id = hc.id
                WHERE hcn.node_name = 'human_case_produce_complete'
                  AND hcn.node_status = 3
                  AND DATE(hcn.node_updated_at) BETWEEN %s AND %s
                  AND hc.deleted_at IS NULL
                  AND hc.produced_by_group = %s
                GROUP BY hc.producer, hc.produced_by_group
            """
            cur.execute(sql_collect, [start_date, end_date, group])
            collect_map = {r["producer"]: r for r in cur.fetchall()}

            sql_qc = """
                SELECT hc.producer,
                    COUNT(*) AS qc_total,
                    SUM(CASE WHEN q.passed = 1 THEN 1 ELSE 0 END) AS qc_passed,
                    SUM(CASE WHEN q.passed = 1 THEN IFNULL(hc.video_seconds, 0) ELSE 0 END) / 3600.0 AS qc_hours
                FROM human_cases hc
                JOIN (
                    SELECT hcn.human_case_id, CASE WHEN hcn.node_status = 3 THEN 1 ELSE 0 END AS passed
                    FROM human_case_nodes hcn
                    WHERE hcn.node_name = 'human_case_sampling' AND hcn.node_status IN (3, 4)
                      AND DATE(hcn.node_updated_at) BETWEEN %s AND %s
                    UNION ALL
                    SELECT hcn.human_case_id, CASE WHEN hcn.node_status = 3 THEN 1 ELSE 0 END AS passed
                    FROM human_case_nodes hcn
                    WHERE hcn.node_name = 'human_case_inspect' AND hcn.node_status IN (3, 4)
                      AND DATE(hcn.node_updated_at) BETWEEN %s AND %s
                      AND hcn.human_case_id NOT IN (
                          SELECT DISTINCT human_case_id FROM human_case_nodes WHERE node_name = 'human_case_sampling'
                      )
                ) q ON q.human_case_id = hc.id
                WHERE hc.deleted_at IS NULL AND hc.produced_by_group = %s
                GROUP BY hc.producer
            """
            cur.execute(sql_qc, [start_date, end_date, start_date, end_date, group])
            qc_map = {r["producer"]: r for r in cur.fetchall()}

            sql_sampling = """
                SELECT hc.producer,
                    COUNT(*) AS sampling_total,
                    SUM(CASE WHEN hcn.node_status = 3 THEN 1 ELSE 0 END) AS sampling_passed,
                    SUM(CASE WHEN hcn.node_status = 3 THEN IFNULL(hc.video_seconds, 0) ELSE 0 END) / 3600.0 AS sampling_hours
                FROM human_cases hc
                JOIN human_case_nodes hcn ON hcn.human_case_id = hc.id
                WHERE hcn.node_name = 'human_case_sampling' AND hcn.node_status IN (3, 4)
                  AND DATE(hcn.node_updated_at) BETWEEN %s AND %s
                  AND hc.deleted_at IS NULL AND hc.produced_by_group = %s
                GROUP BY hc.producer
            """
            cur.execute(sql_sampling, [start_date, end_date, group])
            sampling_map = {r["producer"]: r for r in cur.fetchall()}

            sql_errors = """
                SELECT hc.producer, et.name_cn AS error, COUNT(*) AS cnt
                FROM human_cases hc
                JOIN human_inspect_error_type et ON et.id = hc.inspect_error_type_id
                JOIN human_case_nodes hcn ON hcn.human_case_id = hc.id
                WHERE hcn.node_name = 'human_case_produce_complete' AND hcn.node_status = 3
                  AND DATE(hcn.node_updated_at) BETWEEN %s AND %s
                  AND hc.deleted_at IS NULL AND et.name_cn != '无'
                  AND hc.produced_by_group = %s
                GROUP BY hc.producer, et.name_cn ORDER BY hc.producer, cnt DESC
            """
            cur.execute(sql_errors, [start_date, end_date, group])
            errors_raw = cur.fetchall()
    finally:
        conn.close()

    errors_map = defaultdict(list)
    for r in errors_raw:
        p = r["producer"]
        if len(errors_map[p]) < 5:
            errors_map[p].append({"error": r["error"], "cnt": int(r["cnt"])})

    collectors = []
    all_producers = set(collect_map.keys()) | set(qc_map.keys()) | set(sampling_map.keys())
    for producer in all_producers:
        c = collect_map.get(producer, {})
        q = qc_map.get(producer, {})
        s = sampling_map.get(producer, {})
        qc_passed = int(q.get("qc_passed") or 0)
        qc_total = int(q.get("qc_total") or 0)
        sampling_passed = int(s.get("sampling_passed") or 0)
        sampling_total = int(s.get("sampling_total") or 0)
        collectors.append({
            "name": producer,
            "total_cases": int(c.get("total_cases") or 0),
            "qc_pass_rate": round(qc_passed / qc_total * 100, 1) if qc_total > 0 else 0,
            "sampling_pass_rate": round(sampling_passed / sampling_total * 100, 1) if sampling_total > 0 else 0,
            "total_qc_hours": round(float(q.get("qc_hours") or 0), 1),
            "total_sampling_hours": round(float(s.get("sampling_hours") or 0), 1),
            "top5_errors": errors_map.get(producer, []),
        })
    collectors.sort(key=lambda x: -x["total_cases"])
    result = {"group": group, "collectors": collectors}
    _cache_set(cache_key, result, 300)
    return jsonify(result)


# ═══════════════════════════════════════════════════════════════
#  静态文件
# ═══════════════════════════════════════════════════════════════
@app.route("/")
def index():
    return send_from_directory(".", "index.html")


# ═══════════════════════════════════════════════════════════════
#  项目搜索（Clickhouse）
# ═══════════════════════════════════════════════════════════════
@app.route("/api/projects")
def projects():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify([])
    cache_key = f"projects:{q.lower()}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return jsonify(cached)
    c = ck()
    rows = c.execute(
        "SELECT toString(uuid), name FROM project "
        "WHERE name ILIKE %(q)s ORDER BY name LIMIT 60",
        {"q": f"%{q}%"}
    )
    seen = set()
    result = []
    for r in rows:
        if r[0] not in seen:
            seen.add(r[0])
            result.append({"id": r[0], "name": r[1]})
    _cache_set(cache_key, result, 600)  # 10分钟
    return jsonify(result)


# ═══════════════════════════════════════════════════════════════
#  存量查询（MySQL，与 delivery tracker 计算逻辑完全一致）
# ═══════════════════════════════════════════════════════════════

def _ph(ids):
    return ",".join(["%s"] * len(ids))

def _query_node(cur, project_ids, node_name, node_status):
    """通用单节点查询：video_seconds"""
    ph = _ph(project_ids)
    cur.execute(f"""
        SELECT hc.project_id,
               COUNT(*) AS cnt,
               SUM(IFNULL(hc.video_seconds, 0)) / 3600.0 AS hours
        FROM human_cases hc
        WHERE hc.project_id IN ({ph})
          AND EXISTS (
              SELECT 1 FROM human_case_nodes hcn
              WHERE hcn.human_case_id = hc.id
                AND hcn.node_name = %s AND hcn.node_status = %s
          )
        GROUP BY hc.project_id
    """, project_ids + [node_name, node_status])
    return {r["project_id"]: float(r["hours"] or 0) for r in cur.fetchall()}


def _query_labeling_inprogress(cur, project_ids):
    """标注中（去重）：semantics OR pose，case 级去重"""
    ph = _ph(project_ids)
    cur.execute(f"""
        SELECT hc.project_id,
               COUNT(*) AS cnt,
               SUM(IFNULL(hc.video_seconds, 0)) / 3600.0 AS hours
        FROM human_cases hc
        WHERE hc.project_id IN ({ph})
          AND EXISTS (
              SELECT 1 FROM human_case_nodes hcn
              WHERE hcn.human_case_id = hc.id
                AND hcn.node_name IN ('semantics_labeling', 'pose_labeling')
                AND hcn.node_status = 1
          )
        GROUP BY hc.project_id
    """, project_ids)
    return {r["project_id"]: float(r["hours"] or 0) for r in cur.fetchall()}


def _query_packaged(cur, project_ids):
    """打包完成：complete_job 最新记录 status=3，取 delivery_video_seconds"""
    ph = _ph(project_ids)
    cur.execute(f"""
        SELECT hc.project_id,
               COUNT(*) AS cnt,
               SUM(IFNULL(hc.delivery_video_seconds, 0)) / 3600.0 AS hours
        FROM human_cases hc
        WHERE hc.project_id IN ({ph})
          AND hc.id IN (
              SELECT hcn.human_case_id
              FROM human_case_nodes hcn
              INNER JOIN (
                  SELECT human_case_id, MAX(id) AS max_id
                  FROM human_case_nodes
                  WHERE project_id IN ({ph}) AND node_name = 'complete_job'
                  GROUP BY human_case_id
              ) latest ON hcn.id = latest.max_id
              WHERE hcn.node_status = 3
          )
        GROUP BY hc.project_id
    """, project_ids * 2)
    return {r["project_id"]: float(r["hours"] or 0) for r in cur.fetchall()}


def _query_qc_pending(cur, project_ids):
    """待质检：human_case_inspect status IN (1,2)，取每个 case 最新一条"""
    ph = _ph(project_ids)
    cur.execute(f"""
        SELECT hc.project_id,
               SUM(IFNULL(hc.video_seconds, 0)) / 3600.0 AS hours
        FROM human_cases hc
        INNER JOIN human_case_nodes hcn ON hc.id = hcn.human_case_id
        WHERE hc.project_id IN ({ph})
          AND hcn.node_name = 'human_case_inspect'
          AND hcn.node_status IN (1, 2)
          AND hcn.id IN (
              SELECT MAX(id)
              FROM human_case_nodes
              WHERE project_id IN ({ph}) AND node_name = 'human_case_inspect'
              GROUP BY human_case_id
          )
        GROUP BY hc.project_id
    """, project_ids * 2)
    return {r["project_id"]: float(r["hours"] or 0) for r in cur.fetchall()}


@app.route("/api/stock")
def stock():
    ids_raw = request.args.get("projects", "").strip()
    force   = request.args.get("force", "").strip()
    if not ids_raw:
        return jsonify({})
    project_ids = safe_uuids([p.strip() for p in ids_raw.split(",") if p.strip()])
    if not project_ids:
        return jsonify({})

    cache_key = "stock:" + ",".join(sorted(project_ids))
    if not force:
        cached = _cache_get(cache_key)
        if cached is not None:
            return jsonify(cached)

    # 从 Clickhouse 拿项目名
    in_clause = ",".join(f"'{pid}'" for pid in project_ids)
    c = ck()
    name_rows = c.execute(
        f"SELECT toString(uuid), name FROM project WHERE toString(uuid) IN ({in_clause})"
    )
    names = {r[0]: r[1] for r in name_rows}

    # 从 MySQL 查各阶段存量（与 delivery tracker 完全一致）
    conn = mysql()
    try:
        with conn.cursor() as cur:
            collected_h  = _query_node(cur, project_ids, "human_case_produce",  3)
            qc_pending_h = _query_qc_pending(cur, project_ids)
            qc_passed_h  = _query_node(cur, project_ids, "human_case_inspect",  3)
            sem_ing_h    = _query_node(cur, project_ids, "semantics_labeling",  1)
            pose_ing_h   = _query_node(cur, project_ids, "pose_labeling",       1)
            lab_ing_h    = _query_labeling_inprogress(cur, project_ids)
            labeled_h    = _query_node(cur, project_ids, "labeling_complete",   3)
            packaged_h   = _query_packaged(cur, project_ids)
    finally:
        conn.close()

    result = {}
    for pid in project_ids:
        result[pid] = {
            "name":         names.get(pid, pid),
            "collected_h":  collected_h.get(pid,  0.0),
            "qc_pending_h": qc_pending_h.get(pid, 0.0),
            "qc_passed_h":  qc_passed_h.get(pid,  0.0),
            "sem_ing_h":    sem_ing_h.get(pid,    0.0),
            "pose_ing_h":   pose_ing_h.get(pid,   0.0),
            "lab_ing_h":    lab_ing_h.get(pid,    0.0),
            "labeled_h":    labeled_h.get(pid,    0.0),
            "packaged_h":   packaged_h.get(pid,   0.0),
            "fetched_at":   __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M"),
        }
    _cache_set(cache_key, result, 300)  # 5分钟
    return jsonify(result)


# ═══════════════════════════════════════════════════════════════
#  历史产出（按天聚合）
# ═══════════════════════════════════════════════════════════════

def _query_daily(cur, project_ids, node_name, node_status, start, end):
    """按日聚合某节点某状态的 video_seconds（小时）"""
    ph = _ph(project_ids)
    cur.execute(f"""
        SELECT DATE(hcn.updated_at) AS day,
               SUM(IFNULL(hc.video_seconds, 0)) / 3600.0 AS hours
        FROM human_cases hc
        JOIN human_case_nodes hcn ON hc.id = hcn.human_case_id
        WHERE hc.project_id IN ({ph})
          AND hcn.node_name = %s
          AND hcn.node_status = %s
          AND DATE(hcn.updated_at) BETWEEN %s AND %s
        GROUP BY DATE(hcn.updated_at)
        ORDER BY day
    """, project_ids + [node_name, node_status, start, end])
    return {str(r["day"]): float(r["hours"] or 0) for r in cur.fetchall()}


def _query_daily_mango_dedup(cur, project_ids, node_name, node_status, start, end):
    """Mango专用：按日聚合，同一采集员对同一task只计算一次"""
    ph = _ph(project_ids)
    cur.execute(f"""
        SELECT DATE(first_updated) AS day,
               SUM(video_seconds) / 3600.0 AS hours
        FROM (
            SELECT hc.collector_id, hc.task_id,
                   MIN(hc.video_seconds) AS video_seconds,
                   MIN(hcn.updated_at) AS first_updated
            FROM human_cases hc
            JOIN human_case_nodes hcn ON hc.id = hcn.human_case_id
            WHERE hc.project_id IN ({ph})
              AND hcn.node_name = %s
              AND hcn.node_status = %s
              AND DATE(hcn.updated_at) BETWEEN %s AND %s
            GROUP BY hc.collector_id, hc.task_id
        ) AS dedup
        GROUP BY DATE(first_updated)
        ORDER BY day
    """, project_ids + [node_name, node_status, start, end])
    return {str(r["day"]): float(r["hours"] or 0) for r in cur.fetchall()}


@app.route("/api/history")
def history():
    ids_raw = request.args.get("projects", "").strip()
    start   = request.args.get("start",    "").strip()
    end     = request.args.get("end",      "").strip()
    if not ids_raw or not start or not end:
        return jsonify([])
    project_ids = safe_uuids([p.strip() for p in ids_raw.split(",") if p.strip()])
    if not project_ids:
        return jsonify([])

    conn = mysql()
    try:
        with conn.cursor() as cur:
            collected = _query_daily(cur, project_ids, "human_case_produce",  3, start, end)
            qc_passed = _query_daily(cur, project_ids, "human_case_inspect",  3, start, end)
            labeled   = _query_daily(cur, project_ids, "labeling_complete",   3, start, end)
    finally:
        conn.close()

    # 填满完整日期区间（无数据的天返回 0）
    s, e = date.fromisoformat(start), date.fromisoformat(end)
    result, d = [], s
    while d <= e:
        ds = str(d)
        result.append({
            "date":      ds,
            "collected": collected.get(ds, 0),
            "qc_passed": qc_passed.get(ds, 0),
            "labeled":   labeled.get(ds, 0),
        })
        d += timedelta(days=1)
    return jsonify(result)


# ═══════════════════════════════════════════════════════════════
#  Claude 代理（流式）
# ═══════════════════════════════════════════════════════════════
@app.route("/api/claude", methods=["POST"])
def claude_proxy():
    body = request.get_data()
    data = json.loads(body)
    stream = data.get("stream", False)

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    req = urllib.request.Request(
        LITELLM_URL, data=body, method="POST",
        headers={"Content-Type": "application/json",
                 "Authorization": f"Bearer {LITELLM_KEY}"},
    )

    if stream:
        def generate():
            with urllib.request.urlopen(req, context=ctx) as resp:
                while True:
                    chunk = resp.read(512)
                    if not chunk:
                        break
                    yield chunk
        return Response(generate(), content_type="text/event-stream",
                        headers={"X-Accel-Buffering": "no"})
    else:
        with urllib.request.urlopen(req, context=ctx) as resp:
            return Response(resp.read(), content_type="application/json")


# ═══════════════════════════════════════════════════════════════
#  供应商列表
# ═══════════════════════════════════════════════════════════════
@app.route("/api/vendors")
def get_vendors():
    _require_auth()
    cached = _cache_get("vendors")
    if cached is not None:
        return jsonify(cached)
    conn = mysql()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT produced_by_group AS name
                FROM human_cases
                WHERE produced_by_group IS NOT NULL AND produced_by_group != ''
                ORDER BY produced_by_group
            """)
            vendors = [row["name"] for row in cur.fetchall()]
        _cache_set("vendors", vendors, 3600)  # 1小时
        return jsonify(vendors)
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════
#  采集员统计
# ═══════════════════════════════════════════════════════════════
@app.route("/api/collectors")
def collectors():
    date_str = request.args.get("date", str(date.today()))
    conn = mysql()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT hc.producer, hc.produced_by_group AS vendor,
                       hcn.node_created_at AS t_start, hcn.node_updated_at AS t_end,
                       IFNULL(hc.video_seconds, 0) AS vsec
                FROM human_cases hc
                JOIN human_case_nodes hcn ON hcn.human_case_id = hc.id
                WHERE hcn.node_name = 'human_case_produce_complete'
                  AND hcn.node_status = 3
                  AND DATE(hcn.node_updated_at) = %s
                  AND hc.deleted_at IS NULL
                ORDER BY hc.producer, hcn.node_created_at
            """, [date_str])
            produce_rows = cur.fetchall()

            # 质检通过/总量（新逻辑）
            # - 有 sampling 节点 → 只看 sampling，status=3 才通过
            # - 无 sampling 节点 → 看 human_case_inspect，status=3 通过
            # - 特殊：有 sampling（任意状态）时，inspect=3 不算通过
            cur.execute("""
                SELECT
                    hc.producer,
                    COUNT(*) AS qc_total_cases,
                    SUM(CASE WHEN q.passed = 1 THEN 1 ELSE 0 END) AS qc_passed_cases,
                    SUM(CASE WHEN q.passed = 1 THEN IFNULL(hc.video_seconds, 0) ELSE 0 END) / 3600.0 AS qc_h
                FROM human_cases hc
                JOIN (
                    -- 有 sampling 且已决定（3/4）的 case，今天更新的
                    SELECT hcn.human_case_id,
                           CASE WHEN hcn.node_status = 3 THEN 1 ELSE 0 END AS passed
                    FROM human_case_nodes hcn
                    WHERE hcn.node_name = 'human_case_sampling'
                      AND hcn.node_status IN (3, 4)
                      AND DATE(hcn.node_updated_at) = %s
                    UNION ALL
                    -- 无 sampling 节点的 case，inspect 今天决定的
                    SELECT hcn.human_case_id,
                           CASE WHEN hcn.node_status = 3 THEN 1 ELSE 0 END AS passed
                    FROM human_case_nodes hcn
                    WHERE hcn.node_name = 'human_case_inspect'
                      AND hcn.node_status IN (3, 4)
                      AND DATE(hcn.node_updated_at) = %s
                      AND hcn.human_case_id NOT IN (
                          SELECT DISTINCT human_case_id
                          FROM human_case_nodes
                          WHERE node_name = 'human_case_sampling'
                      )
                ) q ON q.human_case_id = hc.id
                WHERE hc.deleted_at IS NULL
                GROUP BY hc.producer
            """, [date_str, date_str])
            qc_map = {}
            for r in cur.fetchall():
                qc_map[r["producer"]] = {
                    "qc_cases": int(r["qc_passed_cases"] or 0),
                    "qc_total": int(r["qc_total_cases"] or 0),
                    "qc_h":    float(r["qc_h"] or 0),
                }

            # 累计采集时长
            cur.execute("""
                SELECT hc.producer,
                    SUM(IFNULL(hc.video_seconds, 0)) / 3600.0 AS total_collect_h
                FROM human_cases hc
                JOIN human_case_nodes hcn ON hcn.human_case_id = hc.id
                WHERE hcn.node_name = 'human_case_produce_complete'
                  AND hcn.node_status = 3
                  AND hc.deleted_at IS NULL
                GROUP BY hc.producer
            """)
            total_collect_map = {r["producer"]: float(r["total_collect_h"] or 0) for r in cur.fetchall()}

            # 累计质检通过时长 + 通过率
            cur.execute("""
                SELECT hc.producer,
                    COUNT(*) AS total_qc_total,
                    SUM(CASE WHEN q.passed = 1 THEN 1 ELSE 0 END) AS total_qc_passed,
                    SUM(CASE WHEN q.passed = 1 THEN IFNULL(hc.video_seconds, 0) ELSE 0 END) / 3600.0 AS total_qc_h
                FROM human_cases hc
                JOIN (
                    SELECT hcn.human_case_id, CASE WHEN hcn.node_status = 3 THEN 1 ELSE 0 END AS passed
                    FROM human_case_nodes hcn
                    WHERE hcn.node_name = 'human_case_sampling' AND hcn.node_status IN (3, 4)
                    UNION ALL
                    SELECT hcn.human_case_id, CASE WHEN hcn.node_status = 3 THEN 1 ELSE 0 END AS passed
                    FROM human_case_nodes hcn
                    WHERE hcn.node_name = 'human_case_inspect' AND hcn.node_status IN (3, 4)
                      AND hcn.human_case_id NOT IN (
                          SELECT DISTINCT human_case_id FROM human_case_nodes WHERE node_name = 'human_case_sampling'
                      )
                ) q ON q.human_case_id = hc.id
                WHERE hc.deleted_at IS NULL
                GROUP BY hc.producer
            """)
            total_qc_map = {r["producer"]: {
                "total_qc_h": float(r["total_qc_h"] or 0),
                "total_qc_passed": int(r["total_qc_passed"] or 0),
                "total_qc_total": int(r["total_qc_total"] or 0),
            } for r in cur.fetchall()}

            # 待质检时长（全量存量）
            cur.execute("""
                SELECT hc.producer,
                    SUM(IFNULL(hc.video_seconds, 0)) / 3600.0 AS pending_qc_h
                FROM human_cases hc
                JOIN (
                    SELECT hcn.human_case_id FROM human_case_nodes hcn
                    WHERE hcn.node_name = 'human_case_sampling' AND hcn.node_status IN (1, 2)
                    UNION ALL
                    SELECT hcn.human_case_id FROM human_case_nodes hcn
                    WHERE hcn.node_name = 'human_case_inspect' AND hcn.node_status IN (1, 2)
                      AND hcn.human_case_id NOT IN (
                          SELECT DISTINCT human_case_id FROM human_case_nodes WHERE node_name = 'human_case_sampling'
                      )
                ) pending ON pending.human_case_id = hc.id
                WHERE hc.deleted_at IS NULL
                GROUP BY hc.producer
            """)
            pending_qc_map = {r["producer"]: float(r["pending_qc_h"] or 0) for r in cur.fetchall()}

    finally:
        conn.close()

    sessions_by_p = defaultdict(list)
    vendor_by_p   = {}
    vsec_by_p     = defaultdict(float)
    cases_by_p    = defaultdict(int)

    for row in produce_rows:
        p = row["producer"]
        vendor_by_p[p] = row["vendor"] or "未知"
        sessions_by_p[p].append((row["t_start"], row["t_end"]))
        vsec_by_p[p] += float(row["vsec"])
        cases_by_p[p] += 1

    GAP = 30 * 60  # 30 分钟断点阈值

    persons = []
    for p, segs in sessions_by_p.items():
        # node_created_at == node_updated_at（打点型节点），用 t_end 作为时间点
        # 按时间点排序，相邻点间隔 <= GAP 则归为同一活跃段
        points = sorted(set(e for _, e in segs))
        if not points:
            continue
        online_sec = 0
        seg_start = points[0]
        seg_end   = points[0]
        for pt in points[1:]:
            if (pt - seg_end).total_seconds() <= GAP:
                seg_end = pt
            else:
                online_sec += (seg_end - seg_start).total_seconds()
                seg_start = pt
                seg_end   = pt
        online_sec += (seg_end - seg_start).total_seconds()

        c_h = vsec_by_p[p] / 3600
        qc       = qc_map.get(p, {})
        q_h      = float(qc.get("qc_h") or 0)
        qc_pass  = int(qc.get("qc_cases") or 0)
        qc_tot   = int(qc.get("qc_total") or 0)
        tqc      = total_qc_map.get(p, {})
        tqc_passed = int(tqc.get("total_qc_passed") or 0)
        tqc_total  = int(tqc.get("total_qc_total") or 0)
        persons.append({
            "producer":         p,
            "vendor":           vendor_by_p[p],
            # 累计
            "total_collect_h":  round(total_collect_map.get(p, 0), 1),
            "total_qc_h":       round(tqc.get("total_qc_h") or 0, 1),
            "total_qc_rate":    round(tqc_passed / tqc_total * 100, 1) if tqc_total > 0 else 0,
            # 当日
            "collect_cases":    cases_by_p[p],
            "collect_h":        round(c_h, 2),
            "qc_cases":         qc_pass,
            "qc_total":         qc_tot,
            "qc_h":             round(q_h, 2),
            "qc_rate":          round(qc_pass / qc_tot * 100, 1) if qc_tot > 0 else 0,
            # 待质检
            "pending_qc_h":     round(pending_qc_map.get(p, 0), 1),
            # 在线
            "online_h":         round(online_sec / 3600, 2),
            "first_seen":       segs[0][0].strftime("%H:%M"),
            "last_seen":        segs[-1][1].strftime("%H:%M"),
        })

    vendors_map = defaultdict(list)
    for p in persons:
        vendors_map[p["vendor"]].append(p)

    vendors = []
    for vname, vp in vendors_map.items():
        tc      = sum(x["collect_h"] for x in vp)
        tq      = sum(x["qc_h"] for x in vp)
        tqpass  = sum(x["qc_cases"] for x in vp)
        tqtotal = sum(x["qc_total"] for x in vp)
        n       = len(vp)
        vendors.append({
            "vendor":          vname,
            "count":           n,
            "avg_collect_h":   round(tc / n, 2) if n > 0 else 0,
            "total_collect_h": round(tc, 2),
            "total_qc_h":      round(tq, 2),
            "qc_rate":         round(tqpass / tqtotal * 100, 1) if tqtotal > 0 else 0,
            "persons":         sorted(vp, key=lambda x: -x["collect_h"]),
        })
    vendors.sort(key=lambda x: -x["total_collect_h"])

    tc_all      = sum(p["collect_h"] for p in persons)
    tq_all      = sum(p["qc_h"] for p in persons)
    tqpass_all  = sum(p["qc_cases"] for p in persons)
    tqtotal_all = sum(p["qc_total"] for p in persons)
    return jsonify({
        "date":            date_str,
        "total_persons":   len(persons),
        "total_vendors":   len(vendors),
        "total_collect_h": round(tc_all, 2),
        "total_qc_h":      round(tq_all, 2),
        "overall_qc_rate": round(tqpass_all / tqtotal_all * 100, 1) if tqtotal_all > 0 else 0,
        "vendors":         vendors,
    })


@app.route("/api/collector-stats")
def collector_stats():
    """采集员绩效统计（日期范围 + 用户组筛选）"""
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    group = request.args.get("group", "")

    # 缓存 key
    cache_key = f"collector_stats:{start_date}:{end_date}:{group}"
    cached = _cache_get(cache_key)
    if cached:
        return jsonify(cached)

    conn = mysql()
    try:
        with conn.cursor() as cur:
            # 1. 采集完成数据
            sql_collect = """
                SELECT hc.producer, hc.produced_by_group AS grp,
                       COUNT(DISTINCT hc.id) AS total_cases
                FROM human_cases hc
                JOIN human_case_nodes hcn ON hcn.human_case_id = hc.id
                WHERE hcn.node_name = 'human_case_produce_complete'
                  AND hcn.node_status = 3
                  AND DATE(hcn.node_updated_at) BETWEEN %s AND %s
                  AND hc.deleted_at IS NULL
            """
            params_collect = [start_date, end_date]
            if group:
                sql_collect += " AND hc.produced_by_group = %s"
                params_collect.append(group)
            sql_collect += " GROUP BY hc.producer, hc.produced_by_group"

            cur.execute(sql_collect, params_collect)
            collect_map = {r["producer"]: r for r in cur.fetchall()}

            # 2. 质检通过数据（sampling 优先）
            sql_qc = """
                SELECT
                    hc.producer,
                    COUNT(*) AS qc_total,
                    SUM(CASE WHEN q.passed = 1 THEN 1 ELSE 0 END) AS qc_passed,
                    SUM(CASE WHEN q.passed = 1 THEN IFNULL(hc.video_seconds, 0) ELSE 0 END) / 3600.0 AS qc_hours
                FROM human_cases hc
                JOIN (
                    SELECT hcn.human_case_id,
                           CASE WHEN hcn.node_status = 3 THEN 1 ELSE 0 END AS passed
                    FROM human_case_nodes hcn
                    WHERE hcn.node_name = 'human_case_sampling'
                      AND hcn.node_status IN (3, 4)
                      AND DATE(hcn.node_updated_at) BETWEEN %s AND %s
                    UNION ALL
                    SELECT hcn.human_case_id,
                           CASE WHEN hcn.node_status = 3 THEN 1 ELSE 0 END AS passed
                    FROM human_case_nodes hcn
                    WHERE hcn.node_name = 'human_case_inspect'
                      AND hcn.node_status IN (3, 4)
                      AND DATE(hcn.node_updated_at) BETWEEN %s AND %s
                      AND hcn.human_case_id NOT IN (
                          SELECT DISTINCT human_case_id FROM human_case_nodes WHERE node_name = 'human_case_sampling'
                      )
                ) q ON q.human_case_id = hc.id
                WHERE hc.deleted_at IS NULL
            """
            params_qc = [start_date, end_date, start_date, end_date]
            if group:
                sql_qc += " AND hc.produced_by_group = %s"
                params_qc.append(group)
            sql_qc += " GROUP BY hc.producer"

            cur.execute(sql_qc, params_qc)
            qc_map = {r["producer"]: r for r in cur.fetchall()}

            # 3. 抽检通过数据
            sql_sampling = """
                SELECT
                    hc.producer,
                    COUNT(*) AS sampling_total,
                    SUM(CASE WHEN hcn.node_status = 3 THEN 1 ELSE 0 END) AS sampling_passed,
                    SUM(CASE WHEN hcn.node_status = 3 THEN IFNULL(hc.video_seconds, 0) ELSE 0 END) / 3600.0 AS sampling_hours
                FROM human_cases hc
                JOIN human_case_nodes hcn ON hcn.human_case_id = hc.id
                WHERE hcn.node_name = 'human_case_sampling'
                  AND hcn.node_status IN (3, 4)
                  AND DATE(hcn.node_updated_at) BETWEEN %s AND %s
                  AND hc.deleted_at IS NULL
            """
            params_sampling = [start_date, end_date]
            if group:
                sql_sampling += " AND hc.produced_by_group = %s"
                params_sampling.append(group)
            sql_sampling += " GROUP BY hc.producer"

            cur.execute(sql_sampling, params_sampling)
            sampling_map = {r["producer"]: r for r in cur.fetchall()}

            # 4. Top 错误类型（排除"无"，按出现次数降序，取前5）
            sql_errors = """
                SELECT hc.producer, et.name_cn AS error, COUNT(*) AS cnt
                FROM human_cases hc
                JOIN human_inspect_error_type et ON et.id = hc.inspect_error_type_id
                JOIN human_case_nodes hcn ON hcn.human_case_id = hc.id
                WHERE hcn.node_name = 'human_case_produce_complete'
                  AND hcn.node_status = 3
                  AND DATE(hcn.node_updated_at) BETWEEN %s AND %s
                  AND hc.deleted_at IS NULL
                  AND et.name_cn != '无'
            """
            params_errors = [start_date, end_date]
            if group:
                sql_errors += " AND hc.produced_by_group = %s"
                params_errors.append(group)
            sql_errors += " GROUP BY hc.producer, et.name_cn ORDER BY hc.producer, cnt DESC"

            cur.execute(sql_errors, params_errors)
            errors_raw = cur.fetchall()

    finally:
        conn.close()

    # 整理 top errors：每个 producer 取前5
    from collections import defaultdict
    errors_map = defaultdict(list)
    for r in errors_raw:
        p = r["producer"]
        if len(errors_map[p]) < 5:
            errors_map[p].append({"error": r["error"], "cnt": int(r["cnt"])})

    # 合并数据
    collectors = []
    all_producers = set(collect_map.keys()) | set(qc_map.keys()) | set(sampling_map.keys())

    for producer in all_producers:
        c = collect_map.get(producer, {})
        q = qc_map.get(producer, {})
        s = sampling_map.get(producer, {})

        qc_passed = int(q.get("qc_passed") or 0)
        qc_total = int(q.get("qc_total") or 0)
        sampling_passed = int(s.get("sampling_passed") or 0)
        sampling_total = int(s.get("sampling_total") or 0)

        collectors.append({
            "name": producer,
            "group": c.get("grp") or "未知",
            "total_cases": int(c.get("total_cases") or 0),
            "qc_pass_rate": round(qc_passed / qc_total * 100, 1) if qc_total > 0 else 0,
            "sampling_pass_rate": round(sampling_passed / sampling_total * 100, 1) if sampling_total > 0 else 0,
            "total_qc_hours": round(float(q.get("qc_hours") or 0), 1),
            "total_sampling_hours": round(float(s.get("sampling_hours") or 0), 1),
            "avg_daily_hours": 0,
            "top5_errors": errors_map.get(producer, []),
        })

    collectors.sort(key=lambda x: -x["total_cases"])
    return jsonify({"collectors": collectors})


# ═══════════════════════════════════════════════════════════════
#  全盘数据（production_list.xlsx + all_data.py + analysis_all.py）
# ═══════════════════════════════════════════════════════════════

_SCHED_DIR = os.path.dirname(os.path.abspath(__file__))
_REJECTED_PATH = os.path.join(_SCHED_DIR, "rejected_projects.json")

def _load_rejected():
    """返回用户已手动跳过的项目 ID 集合"""
    if os.path.exists(_REJECTED_PATH):
        try:
            with open(_REJECTED_PATH, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except Exception:
            pass
    return set()

def _save_rejected(id_set):
    with open(_REJECTED_PATH, "w", encoding="utf-8") as f:
        json.dump(list(id_set), f, ensure_ascii=False)

# production_list.xlsx 路径（优先同目录，次选桌面运营日报）
_PROD_LIST_PATHS = [
    os.path.join(_SCHED_DIR, "production_list.xlsx"),
    os.path.expanduser("~/Desktop/运营日报/production_list.xlsx"),
]

def _prod_xlsx_path():
    for p in _PROD_LIST_PATHS:
        if os.path.exists(p):
            return p
    return None

def _load_production_list():
    """返回 [{id, name, form, scheme, region, label_ver}]"""
    p = _prod_xlsx_path()
    if not p:
        return []
    wb = openpyxl.load_workbook(p, read_only=True, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    wb.close()
    headers = [str(h).strip() if h else "" for h in rows[0]]
    idx = {h: i for i, h in enumerate(headers)}
    projects = []
    for row in rows[1:]:
        i_id = idx.get("项目id", 1)
        pid = str(row[i_id]).strip() if row[i_id] else ""
        if not pid or not UUID_RE.match(pid):
            continue
        name = str(row[idx.get("项目", 0)] or "").strip()
        projects.append({
            "id":        pid,
            "name":      name,
            "form":      str(row[idx.get("采集形式", 2)] or "").strip(),
            "scheme":    str(row[idx.get("采集方案", 3)] or "").strip(),
            "region":    str(row[idx.get("地域", 4)] or "").strip(),
            "label_ver": str(row[idx.get("标注版本", 5)] or "").strip(),
        })
    return projects


@app.route("/api/overview/new-projects")
def overview_new_projects():
    """返回在 MySQL 里有数据但不在 production_list.xlsx 且未被拒绝的项目"""
    existing_ids = {p["id"] for p in _load_production_list()}
    rejected_ids = _load_rejected()

    conn = mysql()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT project_id, COUNT(*) AS case_count
                FROM human_cases
                WHERE deleted_at IS NULL AND project_id IS NOT NULL
                GROUP BY project_id
                HAVING COUNT(*) >= 5
            """)
            rows = cur.fetchall()
    finally:
        conn.close()

    new_ids = [r["project_id"] for r in rows
               if r["project_id"] and UUID_RE.match(str(r["project_id"]))
               and r["project_id"] not in existing_ids
               and r["project_id"] not in rejected_ids]
    if not new_ids:
        return jsonify([])

    # 从 Clickhouse 取项目名
    in_clause = ",".join(f"'{pid}'" for pid in new_ids[:300])
    try:
        c = ck()
        name_rows = c.execute(
            f"SELECT toString(uuid), name FROM project WHERE toString(uuid) IN ({in_clause})"
        )
        names = {r[0]: r[1] for r in name_rows}
    except Exception:
        names = {}

    case_map = {r["project_id"]: r["case_count"] for r in rows}
    result = [{"id": pid, "name": names.get(pid, ""), "case_count": case_map.get(pid, 0)}
              for pid in new_ids[:300]]
    result.sort(key=lambda x: -x["case_count"])
    return jsonify(result)


@app.route("/api/overview/add-projects", methods=["POST"])
def overview_add_projects():
    """将新项目追加到 production_list.xlsx"""
    projects = request.json  # [{id, name, form, scheme, region, label_ver}]
    if not projects:
        return jsonify({"added": 0})
    xlsx_path = _prod_xlsx_path()
    if not xlsx_path:
        return jsonify({"error": "production_list.xlsx 未找到"}), 404
    wb = openpyxl.load_workbook(xlsx_path)
    ws = wb.active
    for p in projects:
        ws.append([
            p.get("name", ""),
            p.get("id", ""),
            p.get("form", ""),
            p.get("scheme", ""),
            p.get("region", ""),
            p.get("label_ver", ""),
        ])
    wb.save(xlsx_path)
    return jsonify({"added": len(projects)})


@app.route("/api/overview/reject-projects", methods=["POST"])
def overview_reject_projects():
    """将项目 ID 加入永久拒绝列表（下次 new-projects 不再显示）"""
    ids = request.json  # list of id strings
    if not ids:
        return jsonify({"rejected": 0})
    rejected = _load_rejected()
    before = len(rejected)
    rejected.update(str(i) for i in ids if i)
    _save_rejected(rejected)
    return jsonify({"rejected": len(rejected) - before})


@app.route("/api/overview/run")
def overview_run():
    """流式执行 all_data.py → analysis_all.py，SSE 输出进度"""
    all_data_script   = os.path.join(_SCHED_DIR, "all_data.py")
    analysis_script   = os.path.join(_SCHED_DIR, "analysis_all.py")

    def generate():
        for script in [all_data_script, analysis_script]:
            name = os.path.basename(script)
            if not os.path.exists(script):
                yield f"data: ERROR: 找不到脚本 {name}\n\n"
                yield "data: DONE:error\n\n"
                return
            yield f"data: ▶ 运行 {name} ...\n\n"
            proc = subprocess.Popen(
                ["python3", script],
                cwd=_SCHED_DIR,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, bufsize=1,
            )
            for line in proc.stdout:
                line = line.rstrip()
                if line:
                    yield f"data: {line}\n\n"
            proc.wait()
            if proc.returncode != 0:
                yield f"data: ERROR: {name} 退出码 {proc.returncode}\n\n"
                yield "data: DONE:error\n\n"
                return
            yield f"data: ✓ {name} 完成\n\n"
        yield "data: DONE:ok\n\n"

    return Response(generate(), content_type="text/event-stream",
                    headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"})


@app.route("/api/overview/report")
def overview_report():
    """返回最新的 analysis_*.html 报告"""
    html_files = glob.glob(os.path.join(_SCHED_DIR, "analysis_*.html"))
    if not html_files:
        return "暂无报告，请先拉取数据", 404
    latest = max(html_files, key=os.path.getmtime)
    return send_from_directory(_SCHED_DIR, os.path.basename(latest), mimetype="text/html")


@app.route("/api/overview/status")
def overview_status():
    """返回最新报告文件名和时间"""
    html_files = glob.glob(os.path.join(_SCHED_DIR, "analysis_*.html"))
    if not html_files:
        return jsonify({"has_report": False})
    latest = max(html_files, key=os.path.getmtime)
    return jsonify({
        "has_report": True,
        "filename": os.path.basename(latest),
        "mtime": os.path.getmtime(latest),
    })


# ═══════════════════════════════════════════════════════════════
#  全盘每日采集完成量趋势
# ═══════════════════════════════════════════════════════════════
@app.route("/api/overview/daily-collect")
def overview_daily_collect():
    """全盘每日采集完成量（过去 N 天，基于 production_list.xlsx 的项目）"""
    days = min(int(request.args.get("days", 60)), 180)
    end   = date.today()
    start = end - timedelta(days=days - 1)

    projects = _load_production_list()
    if not projects:
        return jsonify([])
    project_ids = [p["id"] for p in projects]

    conn = mysql()
    try:
        with conn.cursor() as cur:
            ph = _ph(project_ids)
            cur.execute(f"""
                SELECT DATE(hcn.node_updated_at) AS day,
                       COUNT(*) AS cases,
                       SUM(IFNULL(hc.video_seconds, 0)) / 3600.0 AS hours
                FROM human_cases hc
                JOIN human_case_nodes hcn ON hcn.human_case_id = hc.id
                WHERE hc.project_id IN ({ph})
                  AND hcn.node_name = 'human_case_produce_complete'
                  AND hcn.node_status = 3
                  AND DATE(hcn.node_updated_at) BETWEEN %s AND %s
                  AND hc.deleted_at IS NULL
                GROUP BY DATE(hcn.node_updated_at)
                ORDER BY day
            """, project_ids + [str(start), str(end)])
            rows = cur.fetchall()
    finally:
        conn.close()

    daily_map = {
        str(r["day"]): {"cases": int(r["cases"]), "hours": round(float(r["hours"] or 0), 2)}
        for r in rows
    }
    result = []
    d = start
    while d <= end:
        ds = str(d)
        info = daily_map.get(ds, {"cases": 0, "hours": 0.0})
        result.append({"date": ds, "cases": info["cases"], "hours": info["hours"]})
        d += timedelta(days=1)
    return jsonify(result)


# ═══════════════════════════════════════════════════════════════
#  API Key 鉴权（供 OpenClaw 等外部工具调用）
# ═══════════════════════════════════════════════════════════════
def _check_api_key():
    """验证 Authorization: Bearer <key>，返回 True 表示通过"""
    if not API_KEY:
        return True
    auth = request.headers.get("Authorization", "")
    return auth == f"Bearer {API_KEY}"


# ═══════════════════════════════════════════════════════════════
#  排期数据持久化
# ═══════════════════════════════════════════════════════════════
_SCHEDULES_PATH = os.path.join(_SCHED_DIR, "schedules.json")

def _load_schedules():
    if os.path.exists(_SCHEDULES_PATH):
        try:
            with open(_SCHEDULES_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"schedules": []}

def _save_schedules(data):
    with open(_SCHEDULES_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def _get_schedule(sid):
    data = _load_schedules()
    for s in data["schedules"]:
        if s["id"] == sid:
            return s, data
    return None, data


# ═══════════════════════════════════════════════════════════════
#  排期 CRUD（浏览器 session 鉴权）
# ═══════════════════════════════════════════════════════════════
@app.route("/api/schedules", methods=["GET"])
def list_schedules():
    data = _load_schedules()
    # 只返回摘要，不返回全量 days 数据
    result = []
    for s in data["schedules"]:
        cur_v = next((v for v in s["versions"] if v["v"] == s["current_version"]), None)
        result.append({
            "id":              s["id"],
            "name":            s["name"],
            "project_ids":     s.get("project_ids", []),
            "project_names":   s.get("project_names", []),
            "current_version": s["current_version"],
            "version_count":   len(s["versions"]),
            "target_h":        cur_v["target_h"] if cur_v else 0,
            "start_date":      cur_v["start_date"] if cur_v else "",
            "saved_at":        cur_v["saved_at"] if cur_v else "",
        })
    return jsonify(result)


@app.route("/api/schedules", methods=["POST"])
def create_schedule():
    body = request.json or {}
    sid  = str(_uuid.uuid4())
    now  = __import__("datetime").datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    schedule = {
        "id":              sid,
        "name":            body.get("name", "新排期"),
        "project_ids":     body.get("project_ids", []),
        "project_names":   body.get("project_names", []),
        "current_version": 1,
        "versions": [{
            "v":          1,
            "saved_at":   now,
            "target_h":   body.get("target_h", 0),
            "start_date": body.get("start_date", ""),
            "params":     body.get("params", {}),
            "days":       body.get("days", []),
        }],
    }
    data = _load_schedules()
    data["schedules"].append(schedule)
    _save_schedules(data)
    return jsonify(schedule), 201


@app.route("/api/schedules/<sid>", methods=["GET"])
def get_schedule(sid):
    s, _ = _get_schedule(sid)
    if not s:
        return jsonify({"error": "not found"}), 404
    return jsonify(s)


@app.route("/api/schedules/<sid>", methods=["PUT"])
def update_schedule(sid):
    """直接覆盖当前版本（单版本模式）"""
    s, data = _get_schedule(sid)
    if not s:
        return jsonify({"error": "not found"}), 404
    body = request.json or {}
    now  = __import__("datetime").datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    version = {
        "v":          1,
        "saved_at":   now,
        "target_h":   body.get("target_h", 0),
        "start_date": body.get("start_date", ""),
        "params":     body.get("params", {}),
        "days":       body.get("days", []),
    }
    s["versions"] = [version]
    s["current_version"] = 1
    if "name"          in body: s["name"]          = body["name"]
    if "project_ids"   in body: s["project_ids"]   = body["project_ids"]
    if "project_names" in body: s["project_names"] = body["project_names"]
    _save_schedules(data)
    return jsonify(s)


@app.route("/api/schedules/<sid>", methods=["DELETE"])
def delete_schedule(sid):
    s, data = _get_schedule(sid)
    if not s:
        return jsonify({"error": "not found"}), 404
    data["schedules"] = [x for x in data["schedules"] if x["id"] != sid]
    _save_schedules(data)
    return jsonify({"deleted": sid})


# ═══════════════════════════════════════════════════════════════
#  排期 × 实际对比（OpenClaw API Key 鉴权 + 浏览器均可访问）
# ═══════════════════════════════════════════════════════════════
def _require_auth():
    """浏览器 session 或 API Key 任一通过即可"""
    if _check_api_key():
        return None
    if not KANBAN_PASSWORD or session.get("authed"):
        return None
    return jsonify({"error": "unauthorized"}), 401


@app.route("/api/schedules/<sid>/actual")
def schedule_actual(sid):
    err = _require_auth()
    if err: return err

    s, _ = _get_schedule(sid)
    if not s:
        return jsonify({"error": "not found"}), 404

    project_ids = safe_uuids(s.get("project_ids", []))
    if not project_ids:
        return jsonify({"error": "no project_ids bound"}), 400

    # 取当前版本参数
    cur_v = next((v for v in s["versions"] if v["v"] == s["current_version"]), None)
    if not cur_v:
        return jsonify({"error": "no version"}), 400

    start = cur_v["start_date"]
    end   = str(date.today())

    conn = mysql()
    try:
        with conn.cursor() as cur:
            collected = _query_daily(cur, project_ids, "human_case_produce_complete", 3, start, end)
            qc_passed = _query_daily(cur, project_ids, "human_case_inspect",          3, start, end)
            labeled   = _query_daily(cur, project_ids, "labeling_complete",           3, start, end)
    finally:
        conn.close()

    # 填满日期区间
    s_d, e_d = date.fromisoformat(start), date.fromisoformat(end)
    rows, d = [], s_d
    while d <= e_d:
        ds = str(d)
        rows.append({
            "date":      ds,
            "collected": round(collected.get(ds, 0), 2),
            "qc_passed": round(qc_passed.get(ds, 0), 2),
            "labeled":   round(labeled.get(ds, 0), 2),
        })
        d += timedelta(days=1)
    return jsonify(rows)


@app.route("/api/schedules/<sid>/compare")
def schedule_compare(sid):
    """计划 vs 实际对比，含偏差和预测"""
    err = _require_auth()
    if err: return err

    s, _ = _get_schedule(sid)
    if not s:
        return jsonify({"error": "not found"}), 404

    project_ids = safe_uuids(s.get("project_ids", []))
    # 允许前端通过 query param 传入（未保存项目关联时的临时使用）
    ids_raw = request.args.get("project_ids", "").strip()
    if ids_raw:
        project_ids = safe_uuids([p.strip() for p in ids_raw.split(",") if p.strip()])
    cur_v = next((v for v in s["versions"] if v["v"] == s["current_version"]), None)
    if not cur_v:
        return jsonify({"error": "no version"}), 400

    params      = cur_v.get("params", {})
    qc_rate     = float(params.get("qc_rate", 85)) / 100
    pack_rate   = float(params.get("pack_rate", 90)) / 100
    target_h    = float(cur_v["target_h"])

    # 计划 days 索引
    plan_by_date = {d["date"]: d for d in cur_v.get("days", [])}

    # 实际查询范围：取排期第一天 ~ 今天
    today = str(date.today())
    if plan_by_date:
        query_start = min(plan_by_date.keys())
    else:
        query_start = (date.today() - timedelta(days=90)).isoformat()

    # 实际产出
    actual_collected, actual_labeled, actual_packed = {}, {}, {}
    if project_ids:
        # 判断是否是Mango项目（检查project_names）
        project_names = s.get("project_names", [])
        is_mango = any("mango" in str(name).lower() for name in project_names)

        conn = mysql()
        try:
            with conn.cursor() as cur:
                if is_mango:
                    actual_collected = _query_daily_mango_dedup(cur, project_ids, "human_case_produce_complete", 3, query_start, today)
                    actual_labeled   = _query_daily_mango_dedup(cur, project_ids, "labeling_complete",           3, query_start, today)
                    actual_packed    = _query_daily_mango_dedup(cur, project_ids, "complete_job",                3, query_start, today)
                else:
                    actual_collected = _query_daily(cur, project_ids, "human_case_produce_complete", 3, query_start, today)
                    actual_labeled   = _query_daily(cur, project_ids, "labeling_complete",           3, query_start, today)
                    actual_packed    = _query_daily(cur, project_ids, "complete_job",                3, query_start, today)
        finally:
            conn.close()

    # 逐天对比（采集量 = 采集完成 × qc_rate，打包完成 = 标注完成 × pack_rate）
    all_dates = sorted(set(list(plan_by_date.keys()) + list(actual_collected.keys()) + list(actual_labeled.keys()) + list(actual_packed.keys())))
    cum_act_collect  = 0.0
    cum_act_label    = 0.0
    rows = []
    for i, ds in enumerate(all_dates):
        pd_ = plan_by_date.get(ds, {})
        plan_l = float(pd_.get("plan_label_h", 0))
        # 实际采集完成 × qc_rate
        act_c  = actual_collected.get(ds, 0.0) * qc_rate
        act_l  = actual_labeled.get(ds, 0.0)
        # 打包完成 = 标注完成 × pack_rate
        act_p  = act_l * pack_rate
        cum_act_collect  += act_c
        cum_act_label    += act_l
        rows.append({
            "date":             ds,
            "actual_collect":   round(act_c,  2),
            "cum_act_collect":  round(cum_act_collect,  2),
            "plan_label":       round(plan_l, 2),
            "actual_label":     round(act_l,  2),
            "actual_packed":    round(act_p,  2),
        })

    # 累计打包完成
    cum_act_packed = sum(actual_packed.get(ds, 0.0) for ds in all_dates if ds <= today)

    # 过去各天的计划累计（截止今天）
    cum_plan_collect_to_today = sum(
        float(plan_by_date.get(ds, {}).get("plan_collect_h", 0)) * qc_rate
        for ds in all_dates if ds <= today
    )
    cum_plan_label_to_today = sum(
        float(plan_by_date.get(ds, {}).get("plan_label_h", 0))
        for ds in all_dates if ds <= today
    )

    # 后续排期可交付（明天开始，计划标注×打包留存率）
    future_deliverable = sum(
        float(plan_by_date.get(ds, {}).get("plan_label_h", 0)) * pack_rate
        for ds in all_dates if ds > today
    )

    return jsonify({
        "schedule_id":              sid,
        "schedule_name":            s["name"],
        "target_h":                 target_h,
        "current_version":          s["current_version"],
        "pack_rate":                pack_rate,
        "cum_act_collect":          round(cum_act_collect,          2),
        "cum_act_label":            round(cum_act_label,            2),
        "cum_act_packed":           round(cum_act_packed,           2),
        "cum_plan_collect_to_today":round(cum_plan_collect_to_today,2),
        "cum_plan_label_to_today":  round(cum_plan_label_to_today,  2),
        "stock_deliverable":        round(cum_act_packed,           2),
        "future_deliverable":       round(future_deliverable,       2),
        "collect_gap":              round(cum_plan_collect_to_today - cum_act_collect, 2),
        "label_gap":                round(cum_plan_label_to_today   - cum_act_label,   2),
        "rows":                     rows,
    })


if __name__ == "__main__":
    print("排期看板服务已启动 → http://localhost:8000")
    app.run(host="0.0.0.0", port=8000, debug=False, threaded=True)
