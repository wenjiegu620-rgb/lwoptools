#!/usr/bin/env python3
"""
delivery-tracker/scripts/manage.py

项目配置管理，Claude 调用命令而非直接编辑 JSON。

子命令：
  list                           列出所有已配置项目
  search-projects --keyword ...  从 ClickHouse 模糊搜索采集项目（名称+UUID）
  add     --name ... --display ... --delivery-date ... [--total-hours ...]
          --envs '<JSON>' --query-projects '<JSON>'
          [--daily-collect-target ... --daily-qc-pass-target ... --daily-label-done-target ...]
  archive --name ...             归档（status → inactive）
  add-mapping --key ... --env ... 新增 env_key → 环境名映射
  set-daily-goals --name ...     设置项目日目标（采集完成/质检通过/标注完成）

envs JSON 格式（数组）：
  [
    {"name": "家居", "target_hours": 100},
    {"name": "家居", "duration_ratio_min": 0.80, "duration_ratio_max": 0.85, "min_task_count": 300}
  ]

query-projects JSON 格式：
  [{"id": "uuid1", "name": "project_name_1"}, ...]
"""

import argparse
import json
import os
import sys

# ── ClickHouse 连接（用于 search-projects）────────────────────────────────────

CK_CONFIG = {
    "host":     os.environ.get("CK_HOST", "10.23.206.206"),
    "port":     int(os.environ.get("CK_PORT", "9000")),
    "database": os.environ.get("CK_DB", "asset"),
    "user":     os.environ.get("CK_USER", "guwenjie"),
    "password": os.environ.get("CK_PASSWORD", "dFGS%4;b)Cg_yMX:vqb#Z-Q_@^Jy"),
}

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "..", "projects.json")


def load_config():
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return json.load(f)


def save_config(config):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
    print(f"✅ 已保存 {CONFIG_PATH}")


# ── search-projects ───────────────────────────────────────────────────────────

def cmd_search_projects(args):
    """从 ClickHouse project 表模糊搜索采集项目，输出 JSON 供 Claude 展示给用户选择"""
    try:
        from clickhouse_driver import Client
    except ImportError:
        print("❌ 缺少依赖：pip install clickhouse-driver", file=sys.stderr)
        sys.exit(1)

    client = Client(**CK_CONFIG)
    keyword = args.keyword.strip()

    rows = client.execute(
        "SELECT uuid, name FROM project WHERE name ILIKE %(kw)s ORDER BY name LIMIT 100",
        {"kw": f"%{keyword}%"},
    )

    if not rows:
        print(json.dumps({"keyword": keyword, "count": 0, "projects": []},
                         ensure_ascii=False, indent=2))
        return

    results = [{"id": str(r[0]), "name": r[1]} for r in rows]
    print(json.dumps({"keyword": keyword, "count": len(results), "projects": results},
                     ensure_ascii=False, indent=2))


# ── list ──────────────────────────────────────────────────────────────────────

def cmd_list(args):
    config = load_config()
    projects = config.get("projects", [])
    if not projects:
        print("（无项目）")
        return

    active   = [p for p in projects if p.get("status") == "active"]
    inactive = [p for p in projects if p.get("status") != "active"]

    def print_proj(p):
        scenes = ", ".join(s["name"] for s in p.get("scenes", []))
        n_qp   = len(p.get("query_projects", []))
        daily = p.get("daily_goals") or {}
        daily_str = (
            f"日目标(采集/质检通过/标注完成)："
            f"{daily.get('collect_done_hours', '—')}h/"
            f"{daily.get('qc_pass_hours', '—')}h/"
            f"{daily.get('label_done_hours', '—')}h"
        )
        print(f"  {p['name']}  ({p.get('display_name', '')})  交付：{p.get('delivery_date','—')}  "
              f"环境：{scenes or '未配置'}  query_projects：{n_qp}个  {daily_str}")

    if active:
        print(f"活跃项目（{len(active)}个）：")
        for p in active:
            print_proj(p)
    if inactive:
        print(f"\n已归档（{len(inactive)}个）：")
        for p in inactive:
            print_proj(p)

    print(f"\nscene_mapping 共 {len(config.get('scene_mapping', {}))} 条")


# ── add ───────────────────────────────────────────────────────────────────────

def cmd_add(args):
    config = load_config()

    # 检查重名
    existing = [p["name"] for p in config.get("projects", [])]
    if args.name in existing:
        print(f"❌ 项目 '{args.name}' 已存在，如需修改请先 archive 再重新 add。")
        sys.exit(1)

    # 解析 envs
    try:
        scenes = json.loads(args.envs)
    except json.JSONDecodeError as e:
        print(f"❌ --envs JSON 格式错误：{e}")
        sys.exit(1)

    # 解析 query-projects
    try:
        query_projects = json.loads(args.query_projects)
    except json.JSONDecodeError as e:
        print(f"❌ --query-projects JSON 格式错误：{e}")
        sys.exit(1)

    proj = {
        "name":           args.name,
        "display_name":   args.display or args.name,
        "delivery_date":  args.delivery_date,
        "scenes":         scenes,
        "query_projects": query_projects,
        "status":         "active",
    }
    if args.total_hours:
        proj["base_total_hours"] = args.total_hours
    daily_goals = {}
    if args.daily_collect_target is not None:
        daily_goals["collect_done_hours"] = args.daily_collect_target
    if args.daily_qc_pass_target is not None:
        daily_goals["qc_pass_hours"] = args.daily_qc_pass_target
    if args.daily_label_done_target is not None:
        daily_goals["label_done_hours"] = args.daily_label_done_target
    if daily_goals:
        proj["daily_goals"] = daily_goals

    config.setdefault("projects", []).append(proj)
    save_config(config)

    print(f"\n已添加项目：{args.name}")
    print(f"  显示名：{proj['display_name']}")
    print(f"  交付日期：{proj['delivery_date']}")
    print(f"  总目标：{args.total_hours or '未设置'}h")
    if daily_goals:
        print(f"  日目标（采集完成/质检通过/标注完成）："
              f"{daily_goals.get('collect_done_hours', '—')}h/"
              f"{daily_goals.get('qc_pass_hours', '—')}h/"
              f"{daily_goals.get('label_done_hours', '—')}h")
    print(f"  环境数：{len(scenes)}")
    print(f"  关联项目数：{len(query_projects)}")


# ── archive ───────────────────────────────────────────────────────────────────

def cmd_archive(args):
    config = load_config()
    for p in config.get("projects", []):
        if p["name"] == args.name:
            p["status"] = "inactive"
            save_config(config)
            print(f"✅ 项目 '{args.name}' 已归档。")
            return
    print(f"❌ 未找到项目 '{args.name}'")
    sys.exit(1)


# ── add-mapping ───────────────────────────────────────────────────────────────

def cmd_add_mapping(args):
    config = load_config()
    mapping = config.setdefault("scene_mapping", {})

    if args.key in mapping:
        old = mapping[args.key]
        if old == args.env:
            print(f"ℹ️  映射已存在：{args.key} → {args.env}，无需更新。")
            return
        print(f"ℹ️  更新映射：{args.key}  {old} → {args.env}")
    else:
        print(f"新增映射：{args.key} → {args.env}")

    mapping[args.key] = args.env
    save_config(config)


def cmd_set_daily_goals(args):
    config = load_config()
    target = None
    for p in config.get("projects", []):
        if p.get("name") == args.name:
            target = p
            break

    if target is None:
        print(f"❌ 未找到项目 '{args.name}'")
        sys.exit(1)

    if args.clear:
        target.pop("daily_goals", None)
        save_config(config)
        print(f"✅ 已清除项目 '{args.name}' 的日目标配置")
        return

    updates = {}
    if args.daily_collect_target is not None:
        updates["collect_done_hours"] = args.daily_collect_target
    if args.daily_qc_pass_target is not None:
        updates["qc_pass_hours"] = args.daily_qc_pass_target
    if args.daily_label_done_target is not None:
        updates["label_done_hours"] = args.daily_label_done_target

    if not updates:
        print("❌ 请至少提供一个目标字段，或使用 --clear 清空。")
        sys.exit(1)

    goals = target.setdefault("daily_goals", {})
    goals.update(updates)
    save_config(config)
    print(
        f"✅ 已更新 '{args.name}' 日目标："
        f"采集完成={goals.get('collect_done_hours', '—')}h, "
        f"质检通过={goals.get('qc_pass_hours', '—')}h, "
        f"标注完成={goals.get('label_done_hours', '—')}h"
    )


# ── 主函数 ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="delivery-tracker 项目配置管理")
    sub = parser.add_subparsers(dest="cmd")

    # list
    sub.add_parser("list", help="列出所有已配置项目")

    # search-projects
    p_search = sub.add_parser("search-projects", help="从 ClickHouse 模糊搜索采集项目")
    p_search.add_argument("--keyword", required=True, help="项目名关键词，如 grape 或 PICO")

    # add
    p_add = sub.add_parser("add", help="新增交付项目")
    p_add.add_argument("--name",           required=True, help="项目唯一标识（英文，如 grape_3000h）")
    p_add.add_argument("--display",        default="",    help="显示名称（中文，如 Grape 3000小时）")
    p_add.add_argument("--delivery-date",  required=True, help="交付日期，格式 YYYY-MM-DD")
    p_add.add_argument("--total-hours",    type=float,    help="总目标时长（小时）")
    p_add.add_argument("--envs",           required=True,
                       help='环境配置 JSON 数组，如 \'[{"name":"家居","duration_ratio_min":0.8,"duration_ratio_max":0.85,"min_task_count":300}]\'')
    p_add.add_argument("--query-projects", required=True,
                       help='关联采集项目 JSON 数组，如 \'[{"id":"uuid","name":"project_name"}]\'')
    p_add.add_argument("--daily-collect-target", type=float, help="日目标：采集完成（小时）")
    p_add.add_argument("--daily-qc-pass-target", type=float, help="日目标：采集质检通过（小时）")
    p_add.add_argument("--daily-label-done-target", type=float, help="日目标：标注完成（小时）")

    # archive
    p_arc = sub.add_parser("archive", help="归档项目")
    p_arc.add_argument("--name", required=True, help="项目名")

    # add-mapping
    p_map = sub.add_parser("add-mapping", help="新增环境映射")
    p_map.add_argument("--key", required=True, help="env_type_name 原始值，如 distribution_center")
    p_map.add_argument("--env", required=True, help="对应中文环境名，如 超市")

    # set-daily-goals
    p_daily = sub.add_parser("set-daily-goals", help="设置项目日目标")
    p_daily.add_argument("--name", required=True, help="项目名")
    p_daily.add_argument("--daily-collect-target", type=float, help="日目标：采集完成（小时）")
    p_daily.add_argument("--daily-qc-pass-target", type=float, help="日目标：采集质检通过（小时）")
    p_daily.add_argument("--daily-label-done-target", type=float, help="日目标：标注完成（小时）")
    p_daily.add_argument("--clear", action="store_true", help="清除日目标配置")

    args = parser.parse_args()

    if args.cmd == "list":
        cmd_list(args)
    elif args.cmd == "search-projects":
        cmd_search_projects(args)
    elif args.cmd == "add":
        cmd_add(args)
    elif args.cmd == "archive":
        cmd_archive(args)
    elif args.cmd == "add-mapping":
        cmd_add_mapping(args)
    elif args.cmd == "set-daily-goals":
        cmd_set_daily_goals(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
