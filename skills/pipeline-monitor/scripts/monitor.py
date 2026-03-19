#!/usr/bin/env python3
"""
pipeline-monitor: 定时监控脚本
由 cron 每 30 分钟调用，查询 Clickhouse，对比快照，判断是否报警。
输出 JSON，openclaw 解析后通过 sessions_send 发到飞书群。
"""

import json
import sys
import os
from datetime import datetime, timedelta
from pathlib import Path

# 添加 scripts 目录到 path，方便引用公共函数
SKILL_DIR = Path(__file__).parent.parent
SNAPSHOT_PATH = SKILL_DIR / "snapshots" / "latest.json"
CONFIG_PATH = SKILL_DIR / "config.json"


def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)


def load_snapshot():
    if SNAPSHOT_PATH.exists():
        with open(SNAPSHOT_PATH) as f:
            return json.load(f)
    return {"time": None, "data": {}, "history": [], "alerts_sent": {}}


def save_snapshot(snapshot):
    SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(SNAPSHOT_PATH, "w") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)


def query_clickhouse(config):
    """查询各项目各节点的最新状态堆积数"""
    try:
        import clickhouse_driver
    except ImportError:
        print("ERROR: clickhouse_driver not installed. Run: pip install clickhouse-driver", file=sys.stderr)
        sys.exit(1)

    ck = config["clickhouse"]
    client = clickhouse_driver.Client(
        host=ck["host"],
        port=ck["port"],
        database=ck["database"],
        user=ck["user"],
        password=ck["password"],
    )

    monitored = config.get("monitored_projects", ["all"])
    if monitored == ["all"] or "all" in monitored:
        project_filter = ""
        params = {}
    else:
        project_filter = "AND multiSearchAnyCaseInsensitive(p.name, %(keywords)s)"
        params = {"keywords": monitored}

    sql = f"""
    SELECT
        p.name AS project_name,
        sub.node_name,
        countIf(sub.latest_status = 'failed') AS failed_cnt,
        countIf(sub.latest_status IN ('running', 'interacting')) AS pending_cnt
    FROM (
        SELECT data_uuid, node_name, project_id,
               argMax(status, updated_at) AS latest_status
        FROM workflow_node_run
        WHERE project_id IN (SELECT toString(uuid) FROM project)
        GROUP BY data_uuid, node_name, project_id
    ) sub
    JOIN project p ON p.uuid = toUUID(sub.project_id)
    WHERE sub.latest_status IN ('failed', 'running', 'interacting')
    {project_filter}
    GROUP BY p.name, sub.node_name
    HAVING failed_cnt > 0
    ORDER BY p.name, failed_cnt DESC
    """

    rows = client.execute(sql, params)
    # rows: [(project_name, node_name, failed_cnt, pending_cnt), ...]
    result = {}
    for project_name, node_name, failed_cnt, pending_cnt in rows:
        if project_name not in result:
            result[project_name] = {}
        result[project_name][node_name] = {
            "failed": int(failed_cnt),
            "pending": int(pending_cnt),
        }
    return result


def query_1h_failure_rate(config, project_name, node_name):
    """查询过去 1 小时失败率（用于观察级判断）"""
    try:
        import clickhouse_driver
    except ImportError:
        return 0.0

    ck = config["clickhouse"]
    client = clickhouse_driver.Client(
        host=ck["host"],
        port=ck["port"],
        database=ck["database"],
        user=ck["user"],
        password=ck["password"],
    )

    sql = """
    SELECT
        countIf(status = 'failed') AS failed_1h,
        count() AS total_1h
    FROM workflow_node_run
    WHERE updated_at >= now() - INTERVAL 1 HOUR
      AND node_name = %(node_name)s
      AND project_id IN (
          SELECT toString(uuid) FROM project WHERE name = %(project_name)s
      )
    """
    rows = client.execute(sql, {"node_name": node_name, "project_name": project_name})
    if rows and rows[0][1] > 0:
        return rows[0][0] / rows[0][1]
    return 0.0


def is_in_silence(alerts_sent, key, silence_hours):
    if key not in alerts_sent:
        return False
    last_alert_time = alerts_sent[key].get("last_alert_time")
    if not last_alert_time:
        return False
    last_alert = datetime.fromisoformat(last_alert_time)
    return datetime.now() - last_alert < timedelta(hours=silence_hours)


def calc_growth(current_data, history, project_name, node_name):
    """计算 1h 内增速（与最早快照对比）"""
    if not history:
        return 0
    oldest = history[-1]  # 最久的快照（约 60 min 前）
    old_failed = oldest.get("data", {}).get(project_name, {}).get(node_name, {}).get("failed", 0)
    current_failed = current_data.get(project_name, {}).get(node_name, {}).get("failed", 0)
    return current_failed - old_failed


def check_alerts(current_data, prev_snapshot, config):
    """
    返回 alerts 列表，每项：
    {
        "level": "warn" | "critical",
        "project": ...,
        "node": ...,
        "failed": ...,
        "growth": ...,
        "owners": [...]   # feishu user_ids，仅 critical 有
    }
    """
    alert_cfg = config["alert"]
    node_owners = config.get("node_owners", {})
    alerts_sent = prev_snapshot.get("alerts_sent", {})
    history = prev_snapshot.get("history", [])
    prev_data = prev_snapshot.get("data", {})

    alerts = []
    new_alerts_sent = {k: v for k, v in alerts_sent.items()}  # 复制

    for project_name, nodes in current_data.items():
        for node_name, counts in nodes.items():
            failed = counts["failed"]
            key = f"{project_name}:{node_name}"
            growth = calc_growth(current_data, history, project_name, node_name)

            # 连续增长计数
            consecutive = 0
            if key in alerts_sent:
                consecutive = alerts_sent[key].get("consecutive_growth", 0)

            prev_failed = prev_data.get(project_name, {}).get(node_name, {}).get("failed", 0)
            if failed > prev_failed:
                consecutive += 1
            else:
                consecutive = 0

            # 更新 consecutive_growth（不管是否报警）
            if key not in new_alerts_sent:
                new_alerts_sent[key] = {}
            new_alerts_sent[key]["consecutive_growth"] = consecutive

            # 静默期检查
            in_silence = is_in_silence(alerts_sent, key, alert_cfg["silence_hours"])

            # 报警判断
            level = None
            if (
                failed >= alert_cfg["critical_threshold"]
                and consecutive >= alert_cfg["critical_consecutive"]
                and not in_silence
            ):
                level = "critical"
            elif (
                failed >= alert_cfg["warn_threshold"]
                and growth >= alert_cfg["warn_growth"]
                and not in_silence
            ):
                level = "warn"

            if level:
                owners = []
                if level == "critical":
                    owner_id = node_owners.get(node_name, "")
                    if owner_id:
                        owners = [owner_id]

                alerts.append({
                    "level": level,
                    "project": project_name,
                    "node": node_name,
                    "failed": failed,
                    "growth": growth,
                    "consecutive": consecutive,
                    "owners": owners,
                })
                new_alerts_sent[key]["last_alert_time"] = datetime.now().isoformat(timespec="seconds")

    return alerts, new_alerts_sent


def format_feishu_message(current_data, alerts):
    """格式化飞书消息"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [f"**[Pipeline Monitor] {now}**\n"]

    if not alerts:
        # 无报警，输出简要汇总
        total_failed = sum(
            counts["failed"]
            for nodes in current_data.values()
            for counts in nodes.values()
        )
        if total_failed == 0:
            lines.append("✅ 所有节点正常，无失败堆积。")
        else:
            lines.append(f"ℹ️ 当前共有 {total_failed} 个失败堆积，未达报警阈值。")
        return "\n".join(lines)

    # 有报警，分级输出
    critical_alerts = [a for a in alerts if a["level"] == "critical"]
    warn_alerts = [a for a in alerts if a["level"] == "warn"]

    if critical_alerts:
        lines.append("🚨 **紧急报警**\n")
        for a in critical_alerts:
            owner_str = ""
            if a["owners"]:
                owner_str = " " + " ".join([f"<at user_id=\"{uid}\"></at>" for uid in a["owners"]])
            lines.append(
                f"- **{a['project']}** / `{a['node']}` — 失败堆积 **{a['failed']}**，"
                f"连续 {a['consecutive']} 次增长{owner_str}"
            )

    if warn_alerts:
        lines.append("\n⚠️ **预警**\n")
        for a in warn_alerts:
            lines.append(
                f"- **{a['project']}** / `{a['node']}` — 失败堆积 {a['failed']}，"
                f"1h 增量 +{a['growth']}"
            )

    return "\n".join(lines)


def build_new_snapshot(current_data, prev_snapshot, new_alerts_sent):
    """构建新快照，history 保留最近 3 条"""
    now = datetime.now().isoformat(timespec="seconds")
    history = prev_snapshot.get("history", [])

    # 把上一条 (prev_snapshot 的 time + data) 推入 history
    if prev_snapshot.get("time"):
        history.insert(0, {
            "time": prev_snapshot["time"],
            "data": prev_snapshot["data"],
        })
    history = history[:3]  # 只保留最近 3 条（90 min）

    return {
        "time": now,
        "data": current_data,
        "history": history,
        "alerts_sent": new_alerts_sent,
    }


def main():
    config = load_config()
    prev_snapshot = load_snapshot()

    # 1. 查询 Clickhouse
    try:
        current_data = query_clickhouse(config)
    except Exception as e:
        print(json.dumps({
            "type": "error",
            "message": f"Clickhouse 查询失败: {e}",
            "feishu_group_id": config["feishu_group_id"],
        }, ensure_ascii=False))
        sys.exit(1)

    # 2. 判断报警
    alerts, new_alerts_sent = check_alerts(current_data, prev_snapshot, config)

    # 3. 保存新快照
    new_snapshot = build_new_snapshot(current_data, prev_snapshot, new_alerts_sent)
    save_snapshot(new_snapshot)

    # 4. 输出结果（openclaw 解析）
    message = format_feishu_message(current_data, alerts)
    output = {
        "type": "monitor_result",
        "feishu_group_id": config["feishu_group_id"],
        "has_alert": len(alerts) > 0,
        "alert_count": len(alerts),
        "message": message,
        "alerts": alerts,
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
