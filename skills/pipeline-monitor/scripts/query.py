#!/usr/bin/env python3
"""
pipeline-monitor: 交互查询脚本
用户在飞书群问节点异常时触发，支持模糊查项目、按节点过滤、多种输出模式。

用法：
  query.py --project <keyword> [--node <name>] [--mode status|detail|trend]
"""

import json
import sys
import argparse
from datetime import datetime, timedelta
from pathlib import Path

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


def get_client(config):
    try:
        import clickhouse_driver
    except ImportError:
        print("ERROR: clickhouse_driver not installed. Run: pip install clickhouse-driver", file=sys.stderr)
        sys.exit(1)
    ck = config["clickhouse"]
    return clickhouse_driver.Client(
        host=ck["host"],
        port=ck["port"],
        database=ck["database"],
        user=ck["user"],
        password=ck["password"],
    )


def query_status(client, project_keyword, node_name=None):
    """各节点堆积概览"""
    node_filter = f"AND sub.node_name = '{node_name}'" if node_name else ""
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
      AND p.name LIKE %(keyword)s
      {node_filter}
    GROUP BY p.name, sub.node_name
    HAVING failed_cnt > 0
    ORDER BY p.name, failed_cnt DESC
    """
    return client.execute(sql, {"keyword": f"%{project_keyword}%"})


def query_detail(client, project_keyword, node_name):
    """某节点失败 case 列表"""
    sql = """
    SELECT
        d.name AS case_name,
        sub.node_name,
        sub.latest_status,
        sub.latest_updated_at
    FROM (
        SELECT data_uuid, node_name, project_id,
               argMax(status, updated_at) AS latest_status,
               max(updated_at) AS latest_updated_at
        FROM workflow_node_run
        WHERE project_id IN (SELECT toString(uuid) FROM project)
        GROUP BY data_uuid, node_name, project_id
    ) sub
    JOIN project p ON p.uuid = toUUID(sub.project_id)
    JOIN data d ON d.uuid = toUUID(sub.data_uuid)
    WHERE sub.latest_status = 'failed'
      AND p.name LIKE %(keyword)s
      AND sub.node_name = %(node_name)s
    ORDER BY sub.latest_updated_at DESC
    LIMIT 50
    """
    return client.execute(sql, {"keyword": f"%{project_keyword}%", "node_name": node_name})


def query_trend(client, project_keyword, node_name=None):
    """最近 7 天每天失败数趋势"""
    node_filter = f"AND node_name = '{node_name}'" if node_name else ""
    sql = f"""
    SELECT
        toDate(updated_at) AS day,
        sub.node_name,
        countIf(sub.latest_status = 'failed') AS failed_cnt
    FROM (
        SELECT data_uuid, node_name, project_id,
               toDate(updated_at) AS day_bucket,
               argMax(status, updated_at) AS latest_status,
               max(updated_at) AS updated_at
        FROM workflow_node_run
        WHERE updated_at >= today() - 7
          AND project_id IN (
              SELECT toString(uuid) FROM project WHERE name LIKE %(keyword)s
          )
          {node_filter}
        GROUP BY data_uuid, node_name, project_id, day_bucket
    ) sub
    GROUP BY day, sub.node_name
    ORDER BY day DESC, failed_cnt DESC
    """
    return client.execute(sql, {"keyword": f"%{project_keyword}%"})


def format_status(rows, project_keyword):
    if not rows:
        return f"项目 `{project_keyword}` 下无失败堆积 ✅"

    lines = [f"**{project_keyword} 节点失败堆积概览**\n"]
    lines.append("| 项目 | 节点 | 失败堆积 | 处理中 |")
    lines.append("|------|------|----------|--------|")
    for project_name, node_name, failed_cnt, pending_cnt in rows:
        lines.append(f"| {project_name} | `{node_name}` | **{failed_cnt}** | {pending_cnt} |")
    return "\n".join(lines)


def format_detail(rows, project_keyword, node_name):
    if not rows:
        return f"项目 `{project_keyword}` / 节点 `{node_name}` 无失败 case ✅"

    lines = [f"**{project_keyword} / {node_name} — 失败 case 列表**（最近 50 条）\n"]
    lines.append("| Case 名称 | 节点 | 失败时间 |")
    lines.append("|-----------|------|----------|")
    for case_name, node, status, updated_at in rows:
        lines.append(f"| {case_name} | `{node}` | {updated_at} |")
    return "\n".join(lines)


def format_trend(rows, project_keyword):
    if not rows:
        return f"项目 `{project_keyword}` 最近 7 天无失败记录 ✅"

    lines = [f"**{project_keyword} — 最近 7 天节点失败趋势**\n"]
    lines.append("| 日期 | 节点 | 失败数 |")
    lines.append("|------|------|--------|")
    for day, node_name, failed_cnt in rows:
        lines.append(f"| {day} | `{node_name}` | {failed_cnt} |")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="pipeline-monitor 交互查询")
    parser.add_argument("--project", required=True, help="项目关键词（模糊匹配）")
    parser.add_argument("--node", default=None, help="节点名称（可选）")
    parser.add_argument(
        "--mode",
        choices=["status", "detail", "trend"],
        default="status",
        help="查询模式：status（默认）/ detail / trend",
    )
    args = parser.parse_args()

    if args.mode == "detail" and not args.node:
        print("ERROR: --mode detail 需要同时指定 --node", file=sys.stderr)
        sys.exit(1)

    config = load_config()
    client = get_client(config)

    try:
        if args.mode == "status":
            rows = query_status(client, args.project, args.node)
            result = format_status(rows, args.project)
        elif args.mode == "detail":
            rows = query_detail(client, args.project, args.node)
            result = format_detail(rows, args.project, args.node)
        elif args.mode == "trend":
            rows = query_trend(client, args.project, args.node)
            result = format_trend(rows, args.project)
    except Exception as e:
        print(json.dumps({
            "type": "error",
            "message": f"查询失败: {e}",
        }, ensure_ascii=False))
        sys.exit(1)

    output = {
        "type": "query_result",
        "mode": args.mode,
        "project": args.project,
        "message": result,
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
