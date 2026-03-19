#!/usr/bin/env python3
"""
运营日报查询脚本

用法:
  python3 query.py              # 今日日报
  python3 query.py --date 2026-03-17   # 指定日期
  python3 query.py --token <jwt>       # 临时指定 token（否则读 config.json）

输出: Markdown 格式日报
"""

import argparse
import json
import os
import sys
import requests
from datetime import date, timedelta
from pathlib import Path
from clickhouse_driver import Client

# ── 配置 ──────────────────────────────────────────────────────────────────────
CONFIG_PATH = Path(__file__).parent.parent / "config.json"

API_BASE    = "https://assetserver.lightwheel.net/api/asset/v1"
API_USER    = "wenjie.gu"

CLIENT_TAGS   = {"Grape", "Orange", "Orange二期", "Mango", "Mango_egodex", "Strawberry"}
WRIST_WF_KEYS = {"wf_E5RT0Jigk62smENT", "wf_1btO60viv624FRgD"}
EGODEX_WF_KEYS = {"wf_G5Zj9XpZo62UzZci"}

EXCLUDE_COLLECT = {"已停采", "已废弃", "归档"}
EXCLUDE_LABEL   = {"已废弃", "归档"}

CLIENT_DISPLAY = {
    "Strawberry": "1X",
    "Mango_egodex": "Mango-EgoDex",
}


def load_config():
    if not CONFIG_PATH.exists():
        return {}
    return json.loads(CONFIG_PATH.read_text())


def get_clickhouse_config(config):
    clickhouse = config.get("clickhouse", {})
    host = os.getenv("DAILY_REPORT_CH_HOST", clickhouse.get("host"))
    port = os.getenv("DAILY_REPORT_CH_PORT", clickhouse.get("port", 9000))
    database = os.getenv("DAILY_REPORT_CH_DB", clickhouse.get("database", "asset"))
    user = os.getenv("DAILY_REPORT_CH_USER", clickhouse.get("user"))
    password = os.getenv("DAILY_REPORT_CH_PASS", clickhouse.get("password"))

    missing = [name for name, value in {
        "host": host,
        "user": user,
        "password": password,
    }.items() if not value]
    if missing:
        missing_str = ", ".join(missing)
        print(
            f"ERROR: Clickhouse 配置缺失: {missing_str}。请在 config.json 或环境变量中配置。",
            file=sys.stderr,
        )
        sys.exit(1)

    return {
        "host": host,
        "port": int(port),
        "database": database,
        "user": user,
        "password": password,
    }


def load_token(config, cli_token=None):
    if cli_token:
        return cli_token
    token = config.get("token", "")
    if token:
        return token
    print("ERROR: 找不到 token，请运行时传入 --token 或在 config.json 中配置", file=sys.stderr)
    sys.exit(1)


def fetch_projects(token):
    """返回 {uuid_str: {client, device, label_ver, tags}}"""
    headers = {"Content-Type": "application/json",
               "authorization": token, "username": API_USER}
    projects = {}
    page = 1
    while True:
        resp = requests.post(
            f"{API_BASE}/project/get",
            headers=headers,
            json={"page": page, "page_size": 100, "project_category": "human_data"},
            timeout=15,
        )
        if resp.status_code == 401:
            print("ERROR: token 已过期，请更新 config.json 中的 token", file=sys.stderr)
            sys.exit(1)
        data = resp.json().get("data", [])
        if not data:
            break
        for p in data:
            tags = {t["tagName"] for t in (p.get("projectTags") or [])}
            clients = {t for t in tags if t in CLIENT_TAGS}
            if not clients:
                continue
            wf_key = (p.get("autoConfig") or {}).get("human_case_workflow_key", "")
            if wf_key in WRIST_WF_KEYS:
                device = "腕部相机"
            elif wf_key in EGODEX_WF_KEYS:
                device = "EgoDex"
            else:
                device = "Pico"
            label_ver = (p.get("autoConfig") or {}).get("labeling_lang_version", "")
            projects[str(p["uuid"])] = {
                "clients":   {CLIENT_DISPLAY.get(c, c) for c in clients},
                "device":    device,
                "label_ver": label_ver,
                "tags":      tags,
            }
        if len(data) < 100:
            break
        page += 1
    return projects


def split_projects(projects):
    """按指标类型拆分 project_id 集合"""
    collect_pids, label_pids, pack_pids = set(), set(), set()
    for uid, meta in projects.items():
        tags = meta["tags"]
        if not (tags & EXCLUDE_LABEL):
            label_pids.add(uid)
            pack_pids.add(uid)
        if not (tags & EXCLUDE_COLLECT):
            collect_pids.add(uid)
    return collect_pids, label_pids, pack_pids


def get_ck(clickhouse_config):
    return Client(
        host=clickhouse_config["host"],
        port=clickhouse_config["port"],
        database=clickhouse_config["database"],
        user=clickhouse_config["user"],
        password=clickhouse_config["password"],
        connect_timeout=10,
        send_receive_timeout=90,
    )


def query_collection(ck, target_date, collect_pids, projects):
    if not collect_pids:
        return {}
    today = target_date.isoformat()
    pids = tuple(collect_pids)

    collect_h = {r[0]: r[1] for r in ck.execute("""
        SELECT project_id, round(sum(video_seconds)/3600, 2)
        FROM (
            SELECT data_uuid, project_id,
                   anyIf(video_seconds, video_seconds > 0) AS video_seconds,
                   min(created_at) AS first_inspect
            FROM (
                SELECT data_uuid, project_id, video_seconds, created_at,
                       workflow_run_id,
                       max(workflow_run_id) OVER (PARTITION BY data_uuid) AS max_run_id
                FROM workflow_node_run
                WHERE node_name = 'human_case_inspect'
                  AND project_id IN %(pids)s
                  AND created_at >= %(today)s
            ) WHERE workflow_run_id = max_run_id
            GROUP BY data_uuid, project_id
            HAVING toDate(first_inspect) = %(today)s
        )
        GROUP BY project_id
    """, {"pids": pids, "today": today})}

    backlog_h = {r[0]: r[1] for r in ck.execute("""
        SELECT project_id, round(sum(video_seconds)/3600, 2)
        FROM (
            SELECT data_uuid, project_id,
                   anyIf(video_seconds, video_seconds > 0) AS video_seconds,
                   argMax(status, updated_at) AS latest_status
            FROM (
                SELECT data_uuid, project_id, video_seconds, status, updated_at,
                       workflow_run_id,
                       max(workflow_run_id) OVER (PARTITION BY data_uuid) AS max_run_id
                FROM workflow_node_run
                WHERE node_name = 'human_case_inspect'
                  AND project_id IN %(pids)s
            ) WHERE workflow_run_id = max_run_id
            GROUP BY data_uuid, project_id
        )
        WHERE latest_status IN ('running', 'interacting')
        GROUP BY project_id
    """, {"pids": pids})}

    pass_h = {r[0]: r[1] for r in ck.execute("""
        SELECT project_id, round(sum(video_seconds)/3600, 2)
        FROM (
            SELECT data_uuid, project_id,
                   anyIf(video_seconds, video_seconds > 0) AS video_seconds,
                   argMax(status, event_time) AS latest_status,
                   max(event_time) AS latest_event
            FROM (
                SELECT data_uuid, project_id, video_seconds, status, event_time,
                       workflow_run_id,
                       max(workflow_run_id) OVER (PARTITION BY data_uuid) AS max_run_id
                FROM workflow_node_run
                WHERE node_name = 'human_case_inspect'
                  AND project_id IN %(pids)s
            ) WHERE workflow_run_id = max_run_id
            GROUP BY data_uuid, project_id
        )
        WHERE latest_status = 'success'
          AND toDate(latest_event) = %(today)s
        GROUP BY project_id
    """, {"pids": pids, "today": today})}

    total_h = {r[0]: r[1] for r in ck.execute("""
        SELECT project_id, round(sum(video_seconds)/3600, 2)
        FROM (
            SELECT data_uuid, project_id,
                   anyIf(video_seconds, video_seconds > 0) AS video_seconds
            FROM workflow_node_run
            WHERE node_name = 'human_case_inspect'
              AND project_id IN %(pids)s
            GROUP BY data_uuid, project_id
        )
        GROUP BY project_id
    """, {"pids": pids})}

    rows = {}
    for pid in collect_pids:
        meta = projects[pid]
        for client in meta["clients"]:
            key = (client, meta["device"])
            row = rows.setdefault(key, [0.0, 0.0, 0.0, 0.0, 0])
            row[0] += collect_h.get(pid, 0)
            row[1] += backlog_h.get(pid, 0)
            row[2] += pass_h.get(pid, 0)
            row[3] += total_h.get(pid, 0)

    from collections import defaultdict

    key_pids = defaultdict(list)
    for pid in collect_pids:
        meta = projects[pid]
        for client in meta["clients"]:
            key_pids[(client, meta["device"])].append(pid)

    for key, group_pids in key_pids.items():
        gpids = tuple(group_pids)
        cnt = ck.execute("""
            SELECT uniqExactIf(producer, producer != '')
            FROM workflow_node_run
            WHERE node_name = 'human_case_produce'
              AND project_id IN %(gpids)s
              AND data_uuid IN (
                  SELECT data_uuid FROM (
                      SELECT data_uuid,
                             min(created_at) AS first_inspect
                      FROM (
                          SELECT data_uuid, created_at,
                                 workflow_run_id,
                                 max(workflow_run_id) OVER (PARTITION BY data_uuid) AS max_run_id
                          FROM workflow_node_run
                          WHERE node_name = 'human_case_inspect'
                            AND project_id IN %(gpids)s
                            AND created_at >= %(today)s
                      ) WHERE workflow_run_id = max_run_id
                      GROUP BY data_uuid
                      HAVING toDate(first_inspect) = %(today)s
                  )
              )
        """, {"gpids": gpids, "today": today})[0][0]
        if key in rows:
            rows[key][4] = cnt

    return rows


def query_collection_by_supplier(ck, target_date, collect_pids):
    if not collect_pids:
        return []
    today = target_date.isoformat()
    pids = tuple(collect_pids)

    rows = ck.execute("""
        SELECT produced_by_group,
               count() AS cases,
               round(sum(video_seconds)/3600, 2) AS hours,
               uniqExactIf(producer, producer != '') AS collectors
        FROM (
            SELECT data_uuid,
                   anyIf(produced_by_group, produced_by_group != '') AS produced_by_group,
                   anyIf(producer, producer != '') AS producer,
                   anyIf(video_seconds, video_seconds > 0) AS video_seconds
            FROM workflow_node_run
            WHERE project_id IN %(pids)s
              AND data_uuid IN (
                  SELECT data_uuid FROM (
                      SELECT data_uuid,
                             min(created_at) AS first_inspect
                      FROM (
                          SELECT data_uuid, created_at,
                                 workflow_run_id,
                                 max(workflow_run_id) OVER (PARTITION BY data_uuid) AS max_run_id
                          FROM workflow_node_run
                          WHERE node_name = 'human_case_inspect'
                            AND project_id IN %(pids)s
                            AND created_at >= %(today)s
                      ) WHERE workflow_run_id = max_run_id
                      GROUP BY data_uuid
                      HAVING toDate(first_inspect) = %(today)s
                  )
              )
            GROUP BY data_uuid
        )
        GROUP BY produced_by_group
        ORDER BY hours DESC
    """, {"pids": pids, "today": today})

    return [(r[0] or "未知", r[1], r[2], r[3]) for r in rows]


def query_labeling(ck, target_date, label_pids, projects):
    if not label_pids:
        return {}
    today = target_date.isoformat()
    pids = tuple(label_pids)

    inflow_h = {r[0]: r[1] for r in ck.execute("""
        SELECT project_id, round(sum(video_seconds)/3600, 2)
        FROM (
            SELECT data_uuid, project_id,
                   anyIf(video_seconds, video_seconds > 0) AS video_seconds,
                   minIf(created_at, node_name='semantics_labeling') AS min_sem,
                   minIf(created_at, node_name='pose_labeling') AS min_pose
            FROM (
                SELECT data_uuid, project_id, video_seconds, created_at, node_name,
                       workflow_run_id,
                       max(workflow_run_id) OVER (PARTITION BY data_uuid) AS max_run_id
                FROM workflow_node_run
                WHERE node_name IN ('semantics_labeling', 'pose_labeling')
                  AND project_id IN %(pids)s
                  AND created_at >= %(today)s
            ) WHERE workflow_run_id = max_run_id
            GROUP BY data_uuid, project_id
            HAVING toDate(min_sem) = %(today)s AND min_pose > 0
        )
        GROUP BY project_id
    """, {"pids": pids, "today": today})}

    sem_h = {r[0]: r[1] for r in ck.execute("""
        SELECT project_id, round(sum(video_seconds)/3600, 2)
        FROM (
            SELECT data_uuid, project_id,
                   anyIf(video_seconds, video_seconds > 0) AS video_seconds,
                   minIf(event_time, status = 'success') AS first_success
            FROM workflow_node_run
            WHERE node_name = 'semantics_labeling'
              AND project_id IN %(pids)s
            GROUP BY data_uuid, project_id
        )
        WHERE toDate(first_success) = %(today)s
        GROUP BY project_id
    """, {"pids": pids, "today": today})}

    pose_h = {r[0]: r[1] for r in ck.execute("""
        SELECT project_id, round(sum(video_seconds)/3600, 2)
        FROM (
            SELECT data_uuid, project_id,
                   anyIf(video_seconds, video_seconds > 0) AS video_seconds,
                   minIf(event_time, status = 'success') AS first_success
            FROM workflow_node_run
            WHERE node_name = 'pose_labeling'
              AND project_id IN %(pids)s
            GROUP BY data_uuid, project_id
        )
        WHERE toDate(first_success) = %(today)s
        GROUP BY project_id
    """, {"pids": pids, "today": today})}

    done_h = {r[0]: r[1] for r in ck.execute("""
        SELECT project_id, round(sum(video_seconds)/3600, 2)
        FROM (
            SELECT data_uuid, project_id,
                   anyIf(video_seconds, video_seconds > 0) AS video_seconds,
                   minIf(event_time, status = 'success') AS first_success
            FROM workflow_node_run
            WHERE node_name = 'labeling_complete'
              AND project_id IN %(pids)s
            GROUP BY data_uuid, project_id
        )
        WHERE toDate(first_success) = %(today)s
        GROUP BY project_id
    """, {"pids": pids, "today": today})}

    rows = {}
    for pid in label_pids:
        ver = projects[pid]["label_ver"] or "未知"
        row = rows.setdefault(ver, [0.0, 0.0, 0.0, 0.0])
        row[0] += inflow_h.get(pid, 0)
        row[1] += sem_h.get(pid, 0)
        row[2] += pose_h.get(pid, 0)
        row[3] += done_h.get(pid, 0)

    return rows


def query_packaging(ck, pack_pids, projects):
    if not pack_pids:
        return {}
    pids = tuple(pack_pids)
    pack_h = {r[0]: r[1] for r in ck.execute("""
        SELECT project_id, round(sum(video_seconds)/3600, 2)
        FROM workflow_node_run
        WHERE node_name = 'delivery_packaging' AND status = 'success'
          AND project_id IN %(pids)s
        GROUP BY project_id
    """, {"pids": pids})}

    rows = {}
    for pid in pack_pids:
        meta = projects[pid]
        for client in meta["clients"]:
            key = (client, meta["device"])
            rows[key] = rows.get(key, 0.0) + pack_h.get(pid, 0)

    return rows


def render(target_date, collect_rows, label_rows):
    lines = [f"## 运营日报 · {target_date}", ""]

    lines += ["### 一、采集 & 质检（今日）", ""]
    if collect_rows:
        lines += ["| 客户 | 设备 | 今日采集完成(h) | 累计采集完成(h) | 待质检(h) | 质检通过(h) | 人效(h/人) | 今日采集人数 |",
                  "|---|---|---|---|---|---|---|---|"]
        for key in sorted(collect_rows):
            client, device = key
            row = collect_rows[key]
            efficiency = round(row[0] / row[4], 2) if row[4] else "—"
            collectors = row[4] if row[4] else "—"
            lines.append(f"| {client} | {device} | {round(row[0],2)} | {round(row[3],2)} | {round(row[1],2)} | {round(row[2],2)} | {efficiency} | {collectors} |")
    else:
        lines.append("今日无符合条件的采集/质检数据。")
    lines.append("")

    lines += ["### 二、标注进度（今日）", ""]
    if label_rows:
        ver_order = sorted(label_rows.keys(), key=lambda v: (v == "未知", v))
        for ver in ver_order:
            row = label_rows[ver]
            throughput = round(row[3] / row[0], 2) if row[0] else "—"
            lines.append(f"**语义版本：{ver}**")
            lines += ["| 指标 | 时长(h) |", "|---|---|"]
            lines.append(f"| 采集流入标注 | {round(row[0],2)} |")
            lines.append(f"| 语义标注完成 | {round(row[1],2)} |")
            lines.append(f"| 手势标注完成 | {round(row[2],2)} |")
            lines.append(f"| 标注完成     | {round(row[3],2)} |")
            lines.append(f"| 标注吞吐率   | {throughput} |")
            lines.append("")
    else:
        lines.append("今日无符合条件的标注数据。")
        lines.append("")

    return "\n".join(lines)


def render_supplier(supplier_rows):
    lines = ["### 附：今日采集供应商明细", ""]
    if supplier_rows:
        lines += ["| 供应商 | 采集完成(h) | 采集人数 | 人效(h/人) |",
                  "|---|---|---|---|"]
        for supplier, cases, hours, collectors in supplier_rows:
            if supplier == "未知" and hours == 0:
                continue
            eff = round(hours / collectors, 2) if collectors else "—"
            lines.append(f"| {supplier} | {hours} | {collectors} | {eff} |")
    else:
        lines.append("今日无供应商采集明细。")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None, help="YYYY-MM-DD，默认今天")
    parser.add_argument("--token", default=None, help="临时指定 JWT token")
    args = parser.parse_args()

    target_date = date.fromisoformat(args.date) if args.date else date.today()
    config = load_config()
    token = load_token(config, args.token)
    clickhouse_config = get_clickhouse_config(config)

    print("正在拉取项目元数据...", file=sys.stderr)
    projects = fetch_projects(token)
    if not projects:
        print("ERROR: 未获取到任何客户项目，请检查 token 或标签配置", file=sys.stderr)
        sys.exit(1)
    print(f"  共 {len(projects)} 个客户项目", file=sys.stderr)

    collect_pids, label_pids, _ = split_projects(projects)
    print(f"  采集统计: {len(collect_pids)} 个项目", file=sys.stderr)
    print(f"  标注统计: {len(label_pids)} 个项目", file=sys.stderr)

    if collect_pids or label_pids:
        print("正在查询 Clickhouse...", file=sys.stderr)
        ck = get_ck(clickhouse_config)
        collect_rows = query_collection(ck, target_date, collect_pids, projects)
        label_rows = query_labeling(ck, target_date, label_pids, projects)
        supplier_rows = query_collection_by_supplier(ck, target_date, collect_pids)
    else:
        print("  无符合过滤条件的项目，跳过 Clickhouse 查询", file=sys.stderr)
        collect_rows = {}
        label_rows = {}
        supplier_rows = []

    print(render(target_date, collect_rows, label_rows))
    print()
    print(render_supplier(supplier_rows))


if __name__ == "__main__":
    main()
