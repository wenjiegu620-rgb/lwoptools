#!/usr/bin/env python3
"""
case_copy/query.py
从 Asset API 拉取 human case，按场景/状态/task去重筛选，可选补充 MySQL 时长。
输出 JSON 供 Claude 展示确认清单。

两种模式：

1. 探索项目场景（让 Claude 做 mapping）：
   python3 query.py --token "Bearer xxx" --username yyy \\
     --project-uuid 774f145e-... --list-scenes

2. 筛选 case：
   python3 query.py --token "Bearer xxx" --username yyy \\
     --project-uuid 774f145e-... \\
     --scene-key home \\       # env_type_name 原始值，由 Claude 从 --list-scenes 结果确定
     --status 3 \\             # 3=质检通过 4=质检不通过
     --count 20 \\
     [--task-dedup] \\
     [--with-duration]
"""

import argparse
import json
import os
import re
import sys
from collections import Counter

import requests


# ── Asset API 工具 ────────────────────────────────────────────────────────────

def make_headers(token: str, username: str) -> dict:
    bearer = token if token.startswith("Bearer") else f"Bearer {token}"
    return {
        "Authorization": bearer,
        "Username": username,
        "Content-Type": "application/json",
    }


def api_base(env: str) -> str:
    return {
        "prod": "https://assetserver.lightwheel.net",
        "dev":  "https://assetserver-dev.lightwheel.net",
    }[env]


def fetch_cases(token: str, username: str, project_uuid: str,
                node_status: int, page_size: int, env: str,
                node_name: str = "human_case_inspect") -> list[dict]:
    body = {
        "page": 1,
        "pageSize": page_size,
        "equal": {"nodeName": node_name, "nodeStatus": node_status},
        "order": [{"updatedAt": 2}],
        "projectUuid": project_uuid,
    }
    resp = requests.post(
        f"{api_base(env)}/api/asset/v2/human-case/list",
        headers=make_headers(token, username),
        json=body,
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json().get("data") or []
    return list(data.values()) if isinstance(data, dict) else data


# ── 场景解析（不依赖硬编码映射）────────────────────────────────────────────────

def parse_scene_key(metadata_str: str) -> str:
    """从 metadata JSON 提取 env_type_name 原始值。未知返回空字符串。"""
    try:
        meta = json.loads(metadata_str) if metadata_str else {}
    except Exception:
        return ""

    # 直接用 env_type_name
    env_type = meta.get("env_type_name", "").strip()
    if env_type:
        return env_type

    # 回退：从 env_num / environment_num 解析前缀
    for field in ["env_num", "environment_num"]:
        val = str(meta.get(field, "")).strip().strip('"')
        if not val or val in ("None", "null"):
            continue
        m = re.match(r"^([a-z_]+?)_x_", val) or re.match(r"^([a-z_]+?)_\d", val)
        if m:
            return m.group(1)

    return ""


# ── MySQL 时长（可选）────────────────────────────────────────────────────────

def fetch_durations(case_ids: list[str]) -> dict[str, float]:
    try:
        import pymysql
    except ImportError:
        print("[warn] pymysql 未安装，跳过时长查询。pip install pymysql", file=sys.stderr)
        return {}

    # 从环境变量读取凭据，若未设置则跳过
    db_host = os.environ.get("CASE_COPY_DB_HOST", "")
    db_user = os.environ.get("CASE_COPY_DB_USER", "")
    db_pass = os.environ.get("CASE_COPY_DB_PASS", "")
    db_port = int(os.environ.get("CASE_COPY_DB_PORT", "3306"))
    if not db_host or not db_user or not db_pass:
        print("[warn] 未设置 CASE_COPY_DB_HOST/USER/PASS 环境变量，跳过时长查询。", file=sys.stderr)
        return {}

    result: dict[str, float] = {}
    try:
        conn = pymysql.connect(
            host=db_host, port=db_port,
            user=db_user, password=db_pass,
            database="asset", charset="utf8mb4", connect_timeout=10,
        )
        for i in range(0, len(case_ids), 200):
            batch = case_ids[i:i + 200]
            ph = ",".join(["%s"] * len(batch))
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT id, delivery_video_seconds, video_seconds "
                    f"FROM human_cases WHERE id IN ({ph})",
                    batch,
                )
                for row in cur.fetchall():
                    cid, dvs, vs = row
                    if dvs and float(dvs) > 0:
                        result[cid] = float(dvs)
                    elif vs and float(vs) > 0:
                        result[cid] = max(float(vs) - 5, 0)
        conn.close()
    except Exception as e:
        print(f"[warn] MySQL 查询失败：{e}", file=sys.stderr)
    return result


# ── 工具函数 ──────────────────────────────────────────────────────────────────

def fmt_duration(seconds: float | None) -> str:
    if not seconds or seconds <= 0:
        return "—"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m{s:02d}s" if h else f"{m}m{s:02d}s"


STATUS_LABELS = {3: "质检通过", 4: "质检不通过"}


# ── 主逻辑 ───────────────────────────────────────────────────────────────────

def cmd_list_scenes(args):
    """探索模式：返回项目内所有 env_type_name 及条数，供 Claude 做自然语言 mapping。"""
    # 拉取质检通过 + 不通过各一批，合并统计场景
    cases = []
    for status in [3, 4]:
        cases += fetch_cases(args.token, args.username, args.project_uuid,
                             status, 200, args.env, args.node_name)

    scene_counter: Counter = Counter()
    for c in cases:
        key = parse_scene_key(c.get("metadata", ""))
        if key:
            scene_counter[key] += 1

    scenes = [{"scene_key": k, "count": v}
              for k, v in scene_counter.most_common()]

    print(json.dumps({
        "project_uuid": args.project_uuid,
        "note": "scene_key 是 env_type_name 原始值，Claude 应根据用户描述匹配对应的 scene_key",
        "scenes": scenes,
    }, ensure_ascii=False, indent=2))


def cmd_query(args):
    """筛选模式：按 scene_key/status/count/task去重 返回 case 清单。"""
    fetch_size = max(args.count * 5, 200)
    raw = fetch_cases(args.token, args.username, args.project_uuid,
                      args.status, fetch_size, args.env, args.node_name)

    # 场景过滤（直接比对 env_type_name 原始值）
    filtered = []
    for c in raw:
        key = parse_scene_key(c.get("metadata", ""))
        c["_scene_key"] = key
        if args.scene_key and key != args.scene_key:
            continue
        filtered.append(c)

    # task 去重
    if args.task_dedup:
        seen: set[str] = set()
        deduped = []
        for c in filtered:
            t = c.get("taskName", "")
            if t not in seen:
                seen.add(t)
                deduped.append(c)
        filtered = deduped

    selected = filtered[:args.count]

    # 可选：补充时长
    durations: dict[str, float] = {}
    if args.with_duration and selected:
        durations = fetch_durations([c["id"] for c in selected])

    total_s = sum(durations.get(c["id"], 0) for c in selected)

    output = []
    for c in selected:
        dur_s = durations.get(c["id"]) if args.with_duration else None
        output.append({
            "id":        c.get("id", ""),
            "name":      c.get("name", ""),
            "task_name": c.get("taskName", ""),
            "scene_key": c["_scene_key"],
            "qc_status": STATUS_LABELS.get(args.status, str(args.status)),
            "duration":  fmt_duration(dur_s) if args.with_duration else None,
        })

    print(json.dumps({
        "count": len(output),
        "total_duration": fmt_duration(total_s) if args.with_duration else None,
        "cases": output,
    }, ensure_ascii=False, indent=2))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--token", required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument("--project-uuid", required=True)
    parser.add_argument("--env", default="prod", choices=["prod", "dev"])

    sub = parser.add_subparsers(dest="cmd")

    # list-scenes 子命令
    ls = sub.add_parser("list-scenes", help="列出项目内所有场景类型及条数")
    ls.add_argument("--node-name", default="human_case_inspect")

    # query 子命令
    q = sub.add_parser("query", help="筛选 case")
    q.add_argument("--node-name", default="human_case_inspect",
                   help="质检节点名称，默认 human_case_inspect")
    q.add_argument("--scene-key", default="",
                   help="env_type_name 原始值（如 home），空=不过滤")
    q.add_argument("--status", type=int, default=3, choices=[3, 4])
    q.add_argument("--count", type=int, default=20)
    q.add_argument("--task-dedup", action="store_true")
    q.add_argument("--with-duration", action="store_true")

    args = parser.parse_args()

    if args.cmd == "list-scenes":
        cmd_list_scenes(args)
    elif args.cmd == "query":
        cmd_query(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
