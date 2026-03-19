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
import sys
import requests
from datetime import date, timedelta
from pathlib import Path
from clickhouse_driver import Client

# ── 配置 ──────────────────────────────────────────────────────────────────────
CONFIG_PATH = Path(__file__).parent.parent / "config.json"

CH_HOST = "10.23.206.206"
CH_PORT = 9000
CH_DB   = "asset"

API_BASE    = "https://assetserver.lightwheel.net/api/asset/v1"
API_USER    = "wenjie.gu"

CLIENT_TAGS   = {"Grape", "Orange", "Orange二期", "Mango", "Mango_egodex", "Strawberry"}
WRIST_WF_KEYS = {"wf_E5RT0Jigk62smENT", "wf_1btO60viv624FRgD"}
EGODEX_WF_KEYS = {"wf_G5Zj9XpZo62UzZci"}

EXCLUDE_COLLECT = {"已停采", "已废弃", "归档"}   # 采集指标排除
EXCLUDE_LABEL   = {"已废弃", "归档"}             # 标注/打包指标排除

# 客户标签显示名映射
CLIENT_DISPLAY = {
    "Strawberry": "1X",
    "Mango_egodex": "Mango-EgoDex",
}


# ── config ────────────────────────────────────────────────────────────────────
def load_config(cli_token=None):
    if not CONFIG_PATH.exists():
        print("ERROR: 找不到 config.json，请参考 SKILL.md Setup 部分配置", file=sys.stderr)
        sys.exit(1)
    cfg = json.loads(CONFIG_PATH.read_text())
    if "ch_user" not in cfg or "ch_pass" not in cfg:
        print("ERROR: config.json 缺少 ch_user / ch_pass 字段", file=sys.stderr)
        sys.exit(1)
    token = cli_token or cfg.get("token", "")
    if not token:
        print("ERROR: token 未配置，请运行时传入 --token 或在 config.json 中配置", file=sys.stderr)
        sys.exit(1)
    return token, cfg["ch_user"], cfg["ch_pass"]


# ── API：拉项目元数据 ──────────────────────────────────────────────────────────
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


# ── CK 客户端 ─────────────────────────────────────────────────────────────────
def get_ck(ch_user, ch_pass):
    return Client(host=CH_HOST, port=CH_PORT, database=CH_DB,
                  user=ch_user, password=ch_pass,
                  connect_timeout=10, send_receive_timeout=90)


# ── 查询：采集 & 质检（今日） ─────────────────────────────────────────────────
def query_collection(ck, target_date, collect_pids, projects):
    next_day = (target_date + timedelta(days=1)).isoformat()
    today    = target_date.isoformat()
    pids     = tuple(collect_pids)

    # 采集成功时长：最新 workflow run 的 inspect 节点，首次 created_at = 今日
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

    # 待质检时长：最新 workflow run 的最新状态为 running/interacting
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

    # 质检通过时长：全局最新 run 的 inspect latest_status=success，且 success 发生在今日
    pass_h = {r[0]: r[1] for r in ck.execute("""
        SELECT project_id, round(sum(video_seconds)/3600, 2)
        FROM (
            SELECT data_uuid, project_id,
                   anyIf(video_seconds, video_seconds > 0) AS video_seconds,
                   argMax(status, event_time)               AS latest_status,
                   max(event_time)                          AS latest_event
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

    # 累计采集完成时长：全量（不限日期），每个 case 只算一次
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

    # 按 client × device 聚合（一个项目可属于多个客户）
    # 先按 (client, device) 分组，采集员人数在组内跨项目去重
    # r = [collect_h, backlog_h, pass_h, total_h, producers]
    rows = {}
    for pid in collect_pids:
        meta = projects[pid]
        for client in meta["clients"]:
            key = (client, meta["device"])
            r   = rows.setdefault(key, [0.0, 0.0, 0.0, 0.0, 0])
            r[0] += collect_h.get(pid, 0)
            r[1] += backlog_h.get(pid, 0)
            r[2] += pass_h.get(pid, 0)
            r[3] += total_h.get(pid, 0)

    # 今日采集员人数：按 (client, device) 分组，跨项目去重
    from collections import defaultdict
    key_pids: dict = defaultdict(list)
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

    return rows  # {(client, device): [collect_h, backlog_h, pass_h, total_h, producers]}


# ── 查询：按供应商采集汇总（今日） ────────────────────────────────────────────
def query_collection_by_supplier(ck, target_date, collect_pids):
    today = target_date.isoformat()
    pids  = tuple(collect_pids)

    rows = ck.execute("""
        SELECT produced_by_group,
               count()                                          AS cases,
               round(sum(video_seconds)/3600, 2)               AS hours,
               uniqExactIf(producer, producer != '')           AS collectors
        FROM (
            SELECT data_uuid,
                   anyIf(produced_by_group, produced_by_group != '') AS produced_by_group,
                   anyIf(producer, producer != '')                    AS producer,
                   anyIf(video_seconds, video_seconds > 0)            AS video_seconds
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
    # [(supplier, cases, hours, collectors)]


# ── 查询：标注进度（今日，按 label_ver） ──────────────────────────────────────
def query_labeling(ck, target_date, label_pids, projects):
    next_day = (target_date + timedelta(days=1)).isoformat()
    today    = target_date.isoformat()
    pids     = tuple(label_pids)

    # 采集流入标注：最新 run 的 semantics 首次 created_at = 今日，且 pose 也存在
    inflow_h = {r[0]: r[1] for r in ck.execute("""
        SELECT project_id, round(sum(video_seconds)/3600, 2)
        FROM (
            SELECT data_uuid, project_id,
                   anyIf(video_seconds, video_seconds > 0)           AS video_seconds,
                   minIf(created_at, node_name='semantics_labeling') AS min_sem,
                   minIf(created_at, node_name='pose_labeling')      AS min_pose
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

    # 语义标注完成：首次 success 发生在今日（重刷不计入）
    sem_h = {r[0]: r[1] for r in ck.execute("""
        SELECT project_id, round(sum(video_seconds)/3600, 2)
        FROM (
            SELECT data_uuid, project_id,
                   anyIf(video_seconds, video_seconds > 0)    AS video_seconds,
                   minIf(event_time, status = 'success')      AS first_success
            FROM workflow_node_run
            WHERE node_name = 'semantics_labeling'
              AND project_id IN %(pids)s
            GROUP BY data_uuid, project_id
        )
        WHERE toDate(first_success) = %(today)s
        GROUP BY project_id
    """, {"pids": pids, "today": today})}

    # 手势标注完成：首次 success 发生在今日（重刷不计入）
    pose_h = {r[0]: r[1] for r in ck.execute("""
        SELECT project_id, round(sum(video_seconds)/3600, 2)
        FROM (
            SELECT data_uuid, project_id,
                   anyIf(video_seconds, video_seconds > 0)    AS video_seconds,
                   minIf(event_time, status = 'success')      AS first_success
            FROM workflow_node_run
            WHERE node_name = 'pose_labeling'
              AND project_id IN %(pids)s
            GROUP BY data_uuid, project_id
        )
        WHERE toDate(first_success) = %(today)s
        GROUP BY project_id
    """, {"pids": pids, "today": today})}

    # 标注完成：首次 success 发生在今日（重刷不计入）
    done_h = {r[0]: r[1] for r in ck.execute("""
        SELECT project_id, round(sum(video_seconds)/3600, 2)
        FROM (
            SELECT data_uuid, project_id,
                   anyIf(video_seconds, video_seconds > 0)    AS video_seconds,
                   minIf(event_time, status = 'success')      AS first_success
            FROM workflow_node_run
            WHERE node_name = 'labeling_complete'
              AND project_id IN %(pids)s
            GROUP BY data_uuid, project_id
        )
        WHERE toDate(first_success) = %(today)s
        GROUP BY project_id
    """, {"pids": pids, "today": today})}

    # 按 label_ver 聚合
    rows = {}
    for pid in label_pids:
        ver = projects[pid]["label_ver"] or "未知"
        r   = rows.setdefault(ver, [0.0, 0.0, 0.0, 0.0])
        r[0] += inflow_h.get(pid, 0)
        r[1] += sem_h.get(pid, 0)
        r[2] += pose_h.get(pid, 0)
        r[3] += done_h.get(pid, 0)

    return rows  # {ver: [inflow_h, sem_h, pose_h, done_h]}


# ── 查询：累计打包（不限日期，按 client × device） ────────────────────────────
def query_packaging(ck, pack_pids, projects):
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
            key  = (client, meta["device"])
            rows[key] = rows.get(key, 0.0) + pack_h.get(pid, 0)

    return rows  # {(client, device): total_h}


# ── 输出 Markdown ─────────────────────────────────────────────────────────────
def render(target_date, collect_rows, label_rows):
    lines = [f"## 运营日报 · {target_date}", ""]

    # 一、采集 & 质检
    lines += ["### 一、采集 & 质检（今日）", ""]
    lines += ["| 客户 | 设备 | 今日采集完成(h) | 累计采集完成(h) | 待质检(h) | 质检通过(h) | 人效(h/人) | 今日采集人数 |",
              "|---|---|---|---|---|---|---|---|"]
    for key in sorted(collect_rows):
        client, device = key
        v = collect_rows[key]
        efficiency = round(v[0] / v[4], 2) if v[4] else "—"
        collectors = v[4] if v[4] else "—"
        lines.append(f"| {client} | {device} | {round(v[0],2)} | {round(v[3],2)} | {round(v[1],2)} | {round(v[2],2)} | {efficiency} | {collectors} |")
    lines.append("")

    # 二、标注进度
    lines += ["### 二、标注进度（今日）", ""]
    ver_order = sorted(label_rows.keys(), key=lambda v: (v == "未知", v))
    for ver in ver_order:
        v = label_rows[ver]
        throughput = round(v[3] / v[0], 2) if v[0] else "—"
        lines.append(f"**语义版本：{ver}**")
        lines += ["| 指标 | 时长(h) |", "|---|---|"]
        lines.append(f"| 采集流入标注 | {round(v[0],2)} |")
        lines.append(f"| 语义标注完成 | {round(v[1],2)} |")
        lines.append(f"| 手势标注完成 | {round(v[2],2)} |")
        lines.append(f"| 标注完成     | {round(v[3],2)} |")
        lines.append(f"| 标注吞吐率   | {throughput} |")
        lines.append("")

    return "\n".join(lines)


def render_supplier(supplier_rows):
    lines = ["### 附：今日采集供应商明细", ""]
    lines += ["| 供应商 | 采集完成(h) | 采集人数 | 人效(h/人) |",
              "|---|---|---|---|"]
    for supplier, cases, hours, collectors in supplier_rows:
        if supplier == "未知" and hours == 0:
            continue
        eff = round(hours / collectors, 2) if collectors else "—"
        lines.append(f"| {supplier} | {hours} | {collectors} | {eff} |")
    return "\n".join(lines)


# ── main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",  default=None, help="YYYY-MM-DD，默认今天")
    parser.add_argument("--token", default=None, help="临时指定 JWT token")
    args = parser.parse_args()

    target_date = date.fromisoformat(args.date) if args.date else date.today()
    token, ch_user, ch_pass = load_config(args.token)

    print(f"正在拉取项目元数据...", file=sys.stderr)
    projects = fetch_projects(token)
    if not projects:
        print("ERROR: 未获取到任何客户项目，请检查 token 或标签配置", file=sys.stderr)
        sys.exit(1)
    print(f"  共 {len(projects)} 个客户项目", file=sys.stderr)

    collect_pids, label_pids, _ = split_projects(projects)
    print(f"  采集统计: {len(collect_pids)} 个项目", file=sys.stderr)
    print(f"  标注统计: {len(label_pids)} 个项目", file=sys.stderr)

    print(f"正在查询 Clickhouse...", file=sys.stderr)
    ck = get_ck(ch_user, ch_pass)
    collect_rows   = query_collection(ck, target_date, collect_pids, projects)
    label_rows     = query_labeling(ck, target_date, label_pids, projects)
    supplier_rows  = query_collection_by_supplier(ck, target_date, collect_pids)

    print(render(target_date, collect_rows, label_rows))
    print()
    print(render_supplier(supplier_rows))


if __name__ == "__main__":
    main()
