#!/usr/bin/env python3
"""
delivery-tracker/scripts/query.py

输出三块报告到 stdout（Markdown），完整数据写文件：
  一、交付进度统计
  二、质检状态（按环境）
  ⚠️  待确认环境（未识别的 env_key，不纳入以上统计）

第三块"建议"由 Claude 根据数据生成。

用法：
  python3 query.py --project grape_2000h
  python3 query.py --all
  python3 query.py --project grape_2000h --no-save
"""

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime

import pymysql
from pymysql.cursors import SSDictCursor

# ── DB 连接 ───────────────────────────────────────────────────────────────────

DB_CONFIG = {
    "host":            os.environ.get("DELIVERY_DB_HOST", "10.23.131.202"),
    "port":            int(os.environ.get("DELIVERY_DB_PORT", "3306")),
    "user":            os.environ.get("DELIVERY_DB_USER", "wenjie.gu"),
    "password":        os.environ.get("DELIVERY_DB_PASSWORD",
                           os.environ.get("ORANGE_WRIST_DB_PASSWORD", "")),
    "database":        "asset",
    "charset":         "utf8mb4",
    "cursorclass":     SSDictCursor,
    "connect_timeout": 60,
    "read_timeout":    300,
}

SCRIPT_DIR    = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH   = os.path.join(SCRIPT_DIR, "..", "projects.json")
SNAPSHOT_FILE = os.path.join(SCRIPT_DIR, "..", "snapshots", "latest.json")
REPORT_DIR    = os.path.expanduser("~/Desktop/delivery_reports")

# ── 环境解析 ──────────────────────────────────────────────────────────────────

def parse_env_key(row: dict) -> str:
    """从查询行提取原始 env key（env_type_name 优先，回退 environment_num/env_num）"""
    v = (row.get("env_type_name") or "").strip().strip('"\'')
    if v and v not in ("None", "null", "nan"):
        return v
    for field in ("environment_num", "env_num"):
        v = str(row.get(field) or "").strip().strip('"\'')
        if not v or v in ("None", "null", "nan"):
            continue
        m = re.match(r"^([a-z_]+?)_x_", v) or re.match(r"^([a-z_]+?)_\d", v)
        if m:
            return m.group(1)
        if re.match(r"^[a-z_]+$", v):
            return v
    return ""


def resolve_scene(env_key: str, scene_mapping: dict):
    """env_key → 中文环境名；未知返回 None（不猜测，不归类）"""
    if not env_key:
        return None
    return scene_mapping.get(env_key)  # None if unknown


# ── SQL 工具 ──────────────────────────────────────────────────────────────────

HOURS_EXPR = (
    "SUM(COALESCE(hc.delivery_video_seconds,"
    " GREATEST(IFNULL(hc.video_seconds, 0) - 5, 0))) / 3600.0"
)
ENV_FIELDS = """
    JSON_UNQUOTE(JSON_EXTRACT(hc.metadata, '$.env_type_name'))   AS env_type_name,
    JSON_UNQUOTE(JSON_EXTRACT(hc.metadata, '$.environment_num')) AS environment_num,
    JSON_UNQUOTE(JSON_EXTRACT(hc.metadata, '$.env_num'))         AS env_num
"""


def _ph(ids):
    return ",".join(["%s"] * len(ids))


def query_node(cur, project_ids, node_name, node_status):
    ph = _ph(project_ids)
    cur.execute(f"""
        SELECT hc.project_id, {ENV_FIELDS},
               COUNT(*) AS cnt, {HOURS_EXPR} AS hours
        FROM human_cases hc
        WHERE hc.project_id IN ({ph})
          AND EXISTS (
              SELECT 1 FROM human_case_nodes hcn
              WHERE hcn.human_case_id = hc.id
                AND hcn.node_name = %s AND hcn.node_status = %s
          )
        GROUP BY hc.project_id, env_type_name, environment_num, env_num
    """, project_ids + [node_name, node_status])
    return list(cur.fetchall())


def query_labeling_inprogress(cur, project_ids):
    """标注中：semantics OR pose，case 级去重"""
    ph = _ph(project_ids)
    cur.execute(f"""
        SELECT hc.project_id, {ENV_FIELDS},
               COUNT(*) AS cnt, {HOURS_EXPR} AS hours
        FROM human_cases hc
        WHERE hc.project_id IN ({ph})
          AND EXISTS (
              SELECT 1 FROM human_case_nodes hcn
              WHERE hcn.human_case_id = hc.id
                AND hcn.node_name IN ('semantics_labeling', 'pose_labeling')
                AND hcn.node_status = 1
          )
        GROUP BY hc.project_id, env_type_name, environment_num, env_num
    """, project_ids)
    return list(cur.fetchall())


def query_packaged(cur, project_ids):
    """打包成功：complete_job 取最新记录 status=3"""
    ph = _ph(project_ids)
    cur.execute(f"""
        SELECT hc.project_id, {ENV_FIELDS},
               COUNT(*) AS cnt, {HOURS_EXPR} AS hours
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
        GROUP BY hc.project_id, env_type_name, environment_num, env_num
    """, project_ids * 2)
    return list(cur.fetchall())


def query_qc_stats(cur, project_ids):
    """质检统计：取每个 case 最新一条 human_case_inspect，按 project+env+status 聚合"""
    ph = _ph(project_ids)
    cur.execute(f"""
        SELECT hc.project_id, {ENV_FIELDS},
               hcn.node_status,
               COUNT(*) AS cnt,
               {HOURS_EXPR} AS hours
        FROM human_cases hc
        INNER JOIN human_case_nodes hcn ON hc.id = hcn.human_case_id
        WHERE hc.project_id IN ({ph})
          AND hcn.node_name = 'human_case_inspect'
          AND hcn.id IN (
              SELECT MAX(id)
              FROM human_case_nodes
              WHERE project_id IN ({ph}) AND node_name = 'human_case_inspect'
              GROUP BY human_case_id
          )
        GROUP BY hc.project_id, env_type_name, environment_num, env_num, hcn.node_status
    """, project_ids * 2)
    return list(cur.fetchall())


# ── 聚合：已知环境 vs 未知环境分开 ───────────────────────────────────────────

def aggregate(rows, scene_mapping):
    """
    返回:
      known   = {scene_name: {"hours": float, "cnt": int}}   # 只含已映射环境
      unknown = {env_key:    {"hours": float, "cnt": int}}   # 未识别，不归入任何环境
    """
    known   = defaultdict(lambda: {"hours": 0.0, "cnt": 0})
    unknown = defaultdict(lambda: {"hours": 0.0, "cnt": 0})
    for row in rows:
        env_key = parse_env_key(row)
        scene   = resolve_scene(env_key, scene_mapping)
        hrs     = float(row.get("hours") or 0)
        cnt     = int(row.get("cnt") or 0)
        if scene is not None:
            known[scene]["hours"] += hrs
            known[scene]["cnt"]   += cnt
        else:
            key = env_key or "(空)"
            unknown[key]["hours"] += hrs
            unknown[key]["cnt"]   += cnt
    return dict(known), dict(unknown)


def h(val):
    return f"{val:.1f}h" if val else "0.0h"


def log(msg):
    print(f"  {msg}", file=sys.stderr)


# ── 主查询 ────────────────────────────────────────────────────────────────────

def run_project(project_config, scene_mapping):
    project_ids = [p["id"] for p in project_config["query_projects"]]

    conn = pymysql.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            log("采集质检成功...")
            qc_pass_k,  qc_pass_u  = aggregate(query_node(cur, project_ids, "human_case_inspect", 3), scene_mapping)
            log("语义标注中...")
            sem_ing_k,  sem_ing_u  = aggregate(query_node(cur, project_ids, "semantics_labeling", 1), scene_mapping)
            log("手势标注中...")
            pose_ing_k, pose_ing_u = aggregate(query_node(cur, project_ids, "pose_labeling", 1), scene_mapping)
            log("标注中（去重）...")
            lab_ing_k,  lab_ing_u  = aggregate(query_labeling_inprogress(cur, project_ids), scene_mapping)
            log("标注完成...")
            lab_done_k, lab_done_u = aggregate(query_node(cur, project_ids, "labeling_complete", 3), scene_mapping)
            log("打包成功...")
            packed_k,   packed_u   = aggregate(query_packaged(cur, project_ids), scene_mapping)
            log("质检统计...")
            qc_rows = query_qc_stats(cur, project_ids)
    finally:
        conn.close()

    # 质检按环境聚合（已知/未知分开）
    qc_known   = defaultdict(lambda: {"pass": 0, "fail": 0, "pending_h": 0.0})
    qc_unknown = defaultdict(lambda: {"pass": 0, "fail": 0, "pending_h": 0.0})
    for row in qc_rows:
        env_key = parse_env_key(row)
        scene   = resolve_scene(env_key, scene_mapping)
        target  = qc_known[scene] if scene is not None else qc_unknown[env_key or "(空)"]
        s = row["node_status"]
        if s == 3:
            target["pass"] += int(row["cnt"] or 0)
        elif s == 4:
            target["fail"] += int(row["cnt"] or 0)
        elif s in (1, 2):
            target["pending_h"] += float(row["hours"] or 0)

    # 合并所有未知 env_key（跨指标）
    all_unknown = defaultdict(lambda: {"hours": 0.0, "cnt": 0})
    for uk in (qc_pass_u, sem_ing_u, pose_ing_u, lab_ing_u, lab_done_u, packed_u):
        for key, v in uk.items():
            all_unknown[key]["hours"] += v["hours"]
            all_unknown[key]["cnt"]   += v["cnt"]
    for key, v in qc_unknown.items():
        all_unknown[key]["cnt"] = max(all_unknown[key]["cnt"],
                                      v["pass"] + v["fail"])

    return {
        "known": {
            "qc_pass":  qc_pass_k,
            "sem_ing":  sem_ing_k,
            "pose_ing": pose_ing_k,
            "lab_ing":  lab_ing_k,
            "lab_done": lab_done_k,
            "packed":   packed_k,
            "qc_scene": dict(qc_known),
        },
        "unknown": dict(all_unknown),
    }


# ── 报告格式化 ────────────────────────────────────────────────────────────────

def build_scene_order(project_config, known_metrics):
    configured = [s["name"] for s in project_config.get("scenes", [])]
    all_scenes = set()
    for m in known_metrics.values():
        if isinstance(m, dict):
            all_scenes.update(m.keys())
    extra = sorted(s for s in all_scenes if s not in configured)
    return configured + extra


def format_report(project_config, data):
    name    = project_config["name"]
    dname   = project_config.get("display_name", name)
    ddate   = project_config.get("delivery_date", "—")
    total_h = project_config.get("base_total_hours") or project_config.get("target_total_hours")

    known   = data["known"]
    unknown = data["unknown"]

    scene_cfg = {s["name"]: s for s in project_config.get("scenes", [])}
    order = build_scene_order(project_config, known)

    def get_h(metric, scene):
        return known[metric].get(scene, {}).get("hours", 0)

    def total(metric):
        return sum(v["hours"] for v in known[metric].values())

    total_packed = total("packed")
    progress_pct = (total_packed / total_h * 100) if total_h and total_h > 0 else None
    status = ("✅" if progress_pct and progress_pct >= 100
              else "⚠️" if progress_pct and progress_pct >= 70
              else "🔴")

    lines = []
    lines.append(f"## {dname} {status}  交付：{ddate}" +
                 (f" | 目标：{total_h}h" if total_h else ""))
    lines.append("")

    # ── 一、交付进度统计 ──
    lines.append("### 一、交付进度统计")
    lines.append("")
    lines.append("| 环境 | 质检成功 | 语义标注中 | 手势标注中 | 标注中 | 标注完成 | 打包成功 | 目标 | 进度 |")
    lines.append("|------|---------|-----------|-----------|--------|---------|---------|------|------|")

    for scene in order:
        qc  = get_h("qc_pass",  scene)
        sem = get_h("sem_ing",  scene)
        pos = get_h("pose_ing", scene)
        lab = get_h("lab_ing",  scene)
        ldn = get_h("lab_done", scene)
        pkg = get_h("packed",   scene)

        tgt = scene_cfg.get(scene, {})
        tgt_h      = tgt.get("target_hours")
        ratio_min  = tgt.get("duration_ratio_min")
        ratio_max  = tgt.get("duration_ratio_max")
        min_tasks  = tgt.get("min_task_count")

        if tgt_h:
            tgt_str  = f"{tgt_h}h"
            prog_str = f"{pkg / tgt_h * 100:.1f}%"
        elif ratio_min and total_h:
            lo = total_h * ratio_min
            hi = total_h * (ratio_max or ratio_min)
            tgt_str = f"{lo:.0f}~{hi:.0f}h"
            curr = pkg / total_packed * 100 if total_packed > 0 else 0
            prog_str = f"占{curr:.1f}% (目标{ratio_min*100:.0f}~{(ratio_max or ratio_min)*100:.0f}%)"
        else:
            tgt_str  = "—"
            prog_str = "—"

        lines.append(
            f"| {scene} | {h(qc)} | {h(sem)} | {h(pos)} | {h(lab)} | {h(ldn)} | {h(pkg)} | {tgt_str} | {prog_str} |"
        )

    lines.append(
        f"| **总计** | **{h(total('qc_pass'))}** | **{h(total('sem_ing'))}** |"
        f" **{h(total('pose_ing'))}** | **{h(total('lab_ing'))}** |"
        f" **{h(total('lab_done'))}** | **{h(total('packed'))}** |"
        f" **{f'{total_h}h' if total_h else '—'}** |"
        f" **{f'{progress_pct:.1f}%' if progress_pct is not None else '—'}** |"
    )
    lines.append("")

    # ── 二、质检状态 ──
    lines.append("### 二、质检状态")
    lines.append("")
    lines.append("| 环境 | 待质检时长 | 质检通过 | 质检失败 | 通过率 |")
    lines.append("|------|-----------|---------|---------|--------|")

    t_pend = t_pass = t_fail = 0.0
    for scene in order:
        s    = known["qc_scene"].get(scene, {})
        pend = s.get("pending_h", 0)
        pas  = s.get("pass", 0)
        fai  = s.get("fail", 0)
        if pas + fai + pend == 0:
            continue
        t_pend += pend
        t_pass += pas
        t_fail += fai
        rate = f"{pas / (pas + fai) * 100:.1f}%" if (pas + fai) > 0 else "—"
        lines.append(f"| {scene} | {h(pend)} | {pas:,} | {fai:,} | {rate} |")

    t_rate = (f"{t_pass / (t_pass + t_fail) * 100:.1f}%"
              if (t_pass + t_fail) > 0 else "—")
    lines.append(
        f"| **总计** | **{h(t_pend)}** | **{int(t_pass):,}** | **{int(t_fail):,}** | **{t_rate}** |"
    )
    lines.append("")

    # ── 待确认环境 ──
    if unknown:
        lines.append("---")
        lines.append("⚠️ **待确认环境**（未纳入以上统计，需确认归类后重新查询）")
        lines.append("")
        lines.append("| env_key | 涉及时长（估） | 涉及条数 |")
        lines.append("|---------|--------------|---------|")
        for key, v in sorted(unknown.items(), key=lambda x: -x[1]["hours"]):
            lines.append(f"| `{key}` | {h(v['hours'])} | {v['cnt']:,} |")
        lines.append("")
        lines.append("确认后运行：`python3 manage.py add-mapping --key <env_key> --scene <环境名>`，然后重新查询。")

    return "\n".join(lines)


# ── 快照 ──────────────────────────────────────────────────────────────────────

def load_snapshot():
    if os.path.exists(SNAPSHOT_FILE):
        try:
            with open(SNAPSHOT_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return None


def save_snapshot(results):
    os.makedirs(os.path.dirname(SNAPSHOT_FILE), exist_ok=True)
    snap = {
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "projects": {
            name: {
                "packed":   {k: v["hours"] for k, v in r["data"]["known"]["packed"].items()},
                "lab_done": {k: v["hours"] for k, v in r["data"]["known"]["lab_done"].items()},
                "qc_scene": r["data"]["known"]["qc_scene"],
            }
            for name, r in results.items()
        },
    }
    with open(SNAPSHOT_FILE, "w", encoding="utf-8") as f:
        json.dump(snap, f, ensure_ascii=False, indent=2)


# ── 主函数 ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", help="项目名")
    parser.add_argument("--all", action="store_true", help="查询所有活跃项目")
    parser.add_argument("--no-save", action="store_true", help="不保存快照")
    args = parser.parse_args()

    with open(CONFIG_PATH, encoding="utf-8") as f:
        config = json.load(f)

    scene_mapping = config.get("scene_mapping", {})
    active = [p for p in config.get("projects", []) if p.get("status") == "active"]

    if not active:
        print("❌ 没有活跃项目，请先通过 manage.py add 添加项目。")
        sys.exit(1)

    if args.project:
        targets_list = [p for p in active if p["name"] == args.project]
        if not targets_list:
            names = ", ".join(p["name"] for p in active)
            print(f"❌ 未找到项目 '{args.project}'，活跃项目：{names}")
            sys.exit(1)
    elif args.all:
        targets_list = active
    else:
        print("当前活跃项目：")
        for i, p in enumerate(active, 1):
            print(f"  {i}. {p['name']} ({p.get('display_name','')}) 交付：{p.get('delivery_date','未知')}")
        print("\n用法：--project <名称> 或 --all")
        sys.exit(0)

    results = {}
    for proj in targets_list:
        name = proj["name"]
        print(f"\n[{name}] 查询中...", file=sys.stderr)
        data = run_project(proj, scene_mapping)
        results[name] = {"config": proj, "data": data}
        print(f"[{name}] 完成", file=sys.stderr)

    if not args.no_save:
        save_snapshot(results)

    # stdout：Markdown 报告
    query_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"> 查询时间：{query_time}\n")
    for name, r in results.items():
        print(format_report(r["config"], r["data"]))
        print()

    # 完整数据写文件
    os.makedirs(REPORT_DIR, exist_ok=True)
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    tag      = args.project if args.project else "all"
    out_path = os.path.join(REPORT_DIR, f"delivery_{tag}_{ts}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({
            "query_time": query_time,
            "projects": {
                name: {
                    "config":   r["config"],
                    "known":    r["data"]["known"],
                    "unknown":  r["data"]["unknown"],
                }
                for name, r in results.items()
            },
        }, f, ensure_ascii=False, indent=2)
    print(f"\n详细数据 → {out_path}")


if __name__ == "__main__":
    main()
