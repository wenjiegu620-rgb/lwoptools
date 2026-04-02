#!/usr/bin/env python3
"""
pipeline-monitor: 定时监控脚本
按节点汇总失败堆积，对比快照计算增量。
触发条件：增量 >= growth_threshold 或 总堆积 >= volume_threshold
"""

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

SKILL_DIR = Path(__file__).parent.parent
SNAPSHOT_PATH = SKILL_DIR / "snapshots" / "latest.json"
CONFIG_PATH = SKILL_DIR / "config.json"
TOP_NODES = 10      # 报警消息最多展示几个节点
TOP_PROJECTS = 3    # 每个节点展示前几个项目


def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)


def load_snapshot():
    if SNAPSHOT_PATH.exists():
        with open(SNAPSHOT_PATH) as f:
            return json.load(f)
    return None


def save_snapshot(snapshot):
    SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(SNAPSHOT_PATH, "w") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)


def query_node_failures(config):
    """按节点+项目汇总失败堆积，返回 {node: {total, projects: [(name, cnt)]}}"""
    try:
        import clickhouse_driver
    except ImportError:
        print("ERROR: pip install clickhouse-driver", file=sys.stderr)
        sys.exit(1)

    ck = config["clickhouse"]
    client = clickhouse_driver.Client(
        host=ck["host"], port=ck["port"],
        database=ck["database"], user=ck["user"], password=ck["password"],
    )

    monitored = config.get("monitored_projects", ["all"])
    if monitored == ["all"] or "all" in monitored:
        project_filter = ""
    else:
        kws = " OR ".join([f"p.name LIKE '%{kw}%'" for kw in monitored])
        project_filter = f"AND ({kws})"

    sql = f"""
    SELECT
        sub.node_name,
        p.name AS project_name,
        count() AS failed_cnt
    FROM (
        SELECT data_uuid, node_name, project_id,
               argMax(status, updated_at) AS latest_status
        FROM workflow_node_run
        WHERE length(project_id) = 36
        GROUP BY data_uuid, node_name, project_id
        HAVING latest_status = 'failed'
    ) sub
    JOIN project p ON p.uuid = toUUID(sub.project_id)
    WHERE 1=1 {project_filter}
    GROUP BY sub.node_name, p.name
    ORDER BY sub.node_name, failed_cnt DESC
    """

    rows = client.execute(sql)

    # 聚合：node -> {total, projects}
    result = {}
    for node_name, project_name, cnt in rows:
        if node_name not in result:
            result[node_name] = {"total": 0, "projects": []}
        result[node_name]["total"] += int(cnt)
        result[node_name]["projects"].append((project_name, int(cnt)))

    return result


def is_silenced(silence, key, silence_hours):
    if key not in silence:
        return False
    last = datetime.fromisoformat(silence[key])
    return datetime.now() - last < timedelta(hours=silence_hours)


def format_message(alerts, node_names):
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [f"🚨 **节点失败报警** [{now}]\n"]
    for a in alerts:
        at_str = ""
        if a.get("owner_id"):
            at_str = f' <at user_id="{a["owner_id"]}"></at>'
        cn = node_names.get(a["node"], "")
        node_display = f"**{cn or a['node']}**"

        # 项目明细
        proj_parts = [f"{p}({c})" for p, c in a["projects"][:TOP_PROJECTS]]
        if len(a["projects"]) > TOP_PROJECTS:
            proj_parts.append(f"等{len(a['projects'])}个项目")
        proj_str = "、".join(proj_parts)

        lines.append(
            f"• {node_display}{at_str} — "
            f"增长 {a['prev_total']}→{a['total']}（+{a['growth']}），"
            f"涉及：{proj_str}，请尽快查看"
        )
    return "\n".join(lines)


def main():
    config = load_config()
    snapshot = load_snapshot()

    alert_cfg = config.get("alert", {})
    silence_hours = alert_cfg.get("silence_hours", 2)
    growth_threshold = alert_cfg.get("growth_threshold", 20)
    volume_threshold = alert_cfg.get("volume_threshold", 50)
    node_owners = config.get("node_owners", {})
    node_names = config.get("node_names", {})
    deadline_hours = alert_cfg.get("todo_deadline_hours", 2)

    try:
        current = query_node_failures(config)
    except Exception as e:
        print(json.dumps({
            "type": "error",
            "has_alert": True,
            "feishu_group_id": config["feishu_group_id"],
            "message": f"Clickhouse 查询失败: {e}",
        }, ensure_ascii=False))
        sys.exit(1)

    # 首次运行：保存快照，不报警
    if snapshot is None:
        save_snapshot({
            "time": datetime.now().isoformat(timespec="seconds"),
            "counts": {k: v["total"] for k, v in current.items()},
            "silence": {},
        })
        print(json.dumps({
            "type": "monitor_result",
            "has_alert": False,
            "feishu_group_id": config["feishu_group_id"],
            "message": "",
            "todos": [],
        }, ensure_ascii=False))
        return

    prev_counts = snapshot.get("counts", {})
    silence = snapshot.get("silence", {})
    new_silence = dict(silence)

    candidates = []
    for node_name, data in current.items():
        total = data["total"]
        if is_silenced(silence, node_name, silence_hours):
            continue
        prev = prev_counts.get(node_name, 0)
        growth = total - prev
        if growth >= growth_threshold or total >= volume_threshold:
            candidates.append({
                "node": node_name,
                "total": total,
                "prev_total": prev,
                "growth": max(growth, 0),
                "projects": data["projects"],
                "owner_id": node_owners.get(node_name, ""),
            })
            new_silence[node_name] = datetime.now().isoformat(timespec="seconds")

    candidates.sort(key=lambda x: x["total"], reverse=True)
    alerts = candidates[:TOP_NODES]
    has_more = len(candidates) > TOP_NODES

    save_snapshot({
        "time": datetime.now().isoformat(timespec="seconds"),
        "counts": {k: v["total"] for k, v in current.items()},
        "silence": new_silence,
    })

    if not alerts:
        print(json.dumps({
            "type": "monitor_result",
            "has_alert": False,
            "feishu_group_id": config["feishu_group_id"],
            "message": "",
            "todos": [],
        }, ensure_ascii=False))
        return

    message = format_message(alerts, node_names)
    if has_more:
        message += f"\n\n_另有 {len(candidates) - TOP_NODES} 个节点触发报警，已省略_"

    # 构造待办列表（有 owner_id 才创建）
    deadline_ms = int((datetime.now() + timedelta(hours=deadline_hours)).timestamp() * 1000)
    todos = []
    for a in alerts:
        if not a.get("owner_id"):
            continue
        cn = node_names.get(a["node"], a["node"])
        proj_str = "、".join(p for p, _ in a["projects"][:TOP_PROJECTS])
        todos.append({
            "owner_id": a["owner_id"],
            "title": f"【节点报警】{cn} 失败堆积 {a['total']}，请尽快处理",
            "notes": (
                f"节点：{cn}（{a['node']}）\n"
                f"失败数：{a['prev_total']} → {a['total']}（+{a['growth']}）\n"
                f"涉及项目：{proj_str}\n"
                f"请在 2 小时内确认并处理"
            ),
            "deadline_ms": deadline_ms,
        })

    print(json.dumps({
        "type": "monitor_result",
        "has_alert": True,
        "alert_count": len(candidates),
        "feishu_group_id": config["feishu_group_id"],
        "message": message,
        "todos": todos,
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
