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
  python3 query.py --tag melon
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
    "host":            os.environ.get(
        "DELIVERY_DB_HOST",
        "rr-uf6y79x928m716yju.mysql.rds.aliyuncs.com",
    ),
    "port":            int(os.environ.get("DELIVERY_DB_PORT", "3306")),
    "user":            os.environ.get("DELIVERY_DB_USER", ""),
    "password":        os.environ.get("DELIVERY_DB_PASSWORD", ""),
    "database":        os.environ.get("DELIVERY_DB_NAME", "asset"),
    "charset":         "utf8mb4",
    "cursorclass":     SSDictCursor,
    "connect_timeout": 60,
    "read_timeout":    300,
}

NEW_DB_CONFIG = {
    "host": os.environ.get(
        "DELIVERY_NEW_DB_HOST",
        "rr-uf6y79x928m716yju.mysql.rds.aliyuncs.com",
    ),
    "port": int(os.environ.get("DELIVERY_NEW_DB_PORT", "3306")),
    "user": os.environ.get("DELIVERY_NEW_DB_USER", ""),
    "password": os.environ.get("DELIVERY_NEW_DB_PASSWORD", ""),
    "database": os.environ.get("DELIVERY_NEW_DB_NAME", "human_case"),
    "charset": "utf8mb4",
    "cursorclass": SSDictCursor,
    "connect_timeout": 60,
    "read_timeout": 300,
    "write_timeout": 60,
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


def resolve_scene(env_key: str, scene_mapping: dict, allow_all: bool = False):
    """env_key → 中文环境名；未知返回 None（不猜测，不归类）"""
    if not env_key:
        return "其他" if allow_all else None
    result = scene_mapping.get(env_key)
    if result is None and allow_all:
        return env_key  # 直接用 env_key 作为场景名
    return result


# ── SQL 工具 ──────────────────────────────────────────────────────────────────

HOURS_EXPR = "SUM(IFNULL(hc.video_seconds, 0)) / 3600.0"
PACKED_HOURS_EXPR = "SUM(COALESCE(hc.delivery_video_seconds, 0)) / 3600.0"
DEDUP_HOURS_EXPR = "SUM(IFNULL(hc.video_seconds, 0)) / 3600.0"

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
          AND hc.deleted_at IS NULL
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
          AND hc.deleted_at IS NULL
          AND EXISTS (
              SELECT 1 FROM human_case_nodes hcn
              WHERE hcn.human_case_id = hc.id
                AND hcn.node_name IN ('semantics_labeling', 'pose_labeling')
                AND hcn.node_status = 1
          )
        GROUP BY hc.project_id, env_type_name, environment_num, env_num
    """, project_ids)
    return list(cur.fetchall())


def query_packaged(cur, project_ids, task_cap_hours=None, packed_node=None):
    """打包成功：取最新记录 status=3；可选仅对打包时长做单 task 封顶"""
    ph = _ph(project_ids)
    node_name = packed_node or "complete_job"
    if task_cap_hours and task_cap_hours > 0:
        picked_sql = f"""
            SELECT hc.project_id, hc.task_name,
                   COALESCE(hc.delivery_video_seconds, 0) AS pack_seconds,
                   {ENV_FIELDS}
            FROM human_cases hc
            WHERE hc.project_id IN ({ph})
          AND hc.deleted_at IS NULL
              AND hc.id IN (
                  SELECT hcn.human_case_id
                  FROM human_case_nodes hcn
                  INNER JOIN (
                      SELECT human_case_id, MAX(id) AS max_id
                      FROM human_case_nodes
                      WHERE project_id IN ({ph}) AND node_name = %s
                      GROUP BY human_case_id
                  ) latest ON hcn.id = latest.max_id
                  WHERE hcn.node_status = 3
              )
        """
        return _execute_capped_rows(cur, picked_sql, project_ids + project_ids + [node_name], packed=True,
                                    task_cap_hours=task_cap_hours)

    cur.execute(f"""
        SELECT hc.project_id, {ENV_FIELDS},
               COUNT(*) AS cnt, {PACKED_HOURS_EXPR} AS hours
        FROM human_cases hc
        WHERE hc.project_id IN ({ph})
          AND hc.deleted_at IS NULL
          AND hc.id IN (
              SELECT hcn.human_case_id
              FROM human_case_nodes hcn
              INNER JOIN (
                  SELECT human_case_id, MAX(id) AS max_id
                  FROM human_case_nodes
                  WHERE project_id IN ({ph}) AND node_name = %s
                  GROUP BY human_case_id
              ) latest ON hcn.id = latest.max_id
              WHERE hcn.node_status = 3
          )
        GROUP BY hc.project_id, env_type_name, environment_num, env_num
    """, project_ids + project_ids + [node_name])
    return list(cur.fetchall())


# ── 去重查询（task_name + producer 各取 1 条）───────────────────────────────

def _dedup_group_expr(dedup_mode: str) -> str:
    if dedup_mode == "task":
        return "hc2.task_name"
    elif dedup_mode == "cap_only":
        return "hc2.id"  # cap_only 不去重，按 id 分组（每行独立）
    else:
        return "hc2.task_name, hc2.producer"


def _dedup_producer_filter(dedup_mode: str, alias: str) -> str:
    if dedup_mode in ("task", "cap_only"):
        return ""  # cap_only 不过滤 producer
    else:
        return f"AND {alias}.producer != ''"


def _execute_capped_rows(cur, picked_sql: str, params: list, packed: bool, task_cap_hours,
                         defer_cap: bool = False):
    hours_col = "pack_seconds" if packed else "video_seconds"
    cur.execute(picked_sql, params)
    picked_rows = list(cur.fetchall())
    if not picked_rows:
        return []

    # defer_cap=True: 不封顶、不聚合，直接返回含 task_name 的原始行供上层全局封顶
    if defer_cap:
        out = []
        for row in picked_rows:
            out.append({
                "project_id": row.get("project_id"),
                "env_type_name": row.get("env_type_name"),
                "environment_num": row.get("environment_num"),
                "env_num": row.get("env_num"),
                "task_name": row.get("task_name") or "",
                "cnt": 1,
                "hours": float(row.get(hours_col) or 0.0) / 3600.0,
            })
        return out

    task_totals = {}
    cap_seconds = None
    if task_cap_hours and task_cap_hours > 0:
        cap_seconds = float(task_cap_hours) * 3600.0
        for row in picked_rows:
            task_key = row.get("task_name") or ""
            task_totals[task_key] = task_totals.get(task_key, 0.0) + float(row.get(hours_col) or 0.0)

    grouped = {}
    for row in picked_rows:
        key = (
            row.get("project_id"),
            row.get("env_type_name"),
            row.get("environment_num"),
            row.get("env_num"),
        )
        sec = float(row.get(hours_col) or 0.0)
        if cap_seconds:
            task_key = row.get("task_name") or ""
            total_sec = task_totals.get(task_key, 0.0)
            if total_sec > 0:
                sec *= min(1.0, cap_seconds / total_sec)
            else:
                sec = 0.0

        if key not in grouped:
            grouped[key] = {"cnt": 0, "sec": 0.0}
        grouped[key]["cnt"] += 1
        grouped[key]["sec"] += sec

    out = []
    for (project_id, env_type_name, environment_num, env_num), data in grouped.items():
        out.append({
            "project_id": project_id,
            "env_type_name": env_type_name,
            "environment_num": environment_num,
            "env_num": env_num,
            "cnt": data["cnt"],
            "hours": data["sec"] / 3600.0,
        })
    return out


def query_node_dedup(cur, project_ids, node_name, node_status,
                     dedup_mode="task_producer", task_cap_hours=None):
    """节点统计（去重版）：支持 task+producer 或 task 去重，并可按 task 时长封顶"""
    ph = _ph(project_ids)
    group_expr = _dedup_group_expr(dedup_mode)
    outer_prod = _dedup_producer_filter(dedup_mode, "hc")
    inner_prod = _dedup_producer_filter(dedup_mode, "hc2")
    picked_sql = f"""
        SELECT hc.project_id, hc.task_name, IFNULL(hc.video_seconds, 0) AS video_seconds,
               {ENV_FIELDS}
        FROM human_cases hc
        WHERE hc.project_id IN ({ph})
          AND hc.deleted_at IS NULL
          {outer_prod}
          AND hc.id IN (
              SELECT MAX(hc2.id)
              FROM human_cases hc2
              WHERE hc2.project_id IN ({ph})
                AND hc2.deleted_at IS NULL
                {inner_prod}
                AND EXISTS (
                    SELECT 1 FROM human_case_nodes hcn2
                    WHERE hcn2.human_case_id = hc2.id
                      AND hcn2.node_name = %s AND hcn2.node_status = %s
                )
              GROUP BY {group_expr}
          )
    """
    params = project_ids + project_ids + [node_name, node_status]
    return _execute_capped_rows(cur, picked_sql, params, packed=False,
                                task_cap_hours=task_cap_hours)


def query_qc_pass_dedup_compat(cur, project_ids, dedup_mode="task_producer",
                               task_cap_hours=None):
    """
    采集质检成功（去重版）：
      - dedup_mode=task_producer: 同 task_name + producer 取 1 条
      - dedup_mode=task: 同 task_name 取 1 条
      - task_cap_hours: 每个 task 总时长封顶（小时）
    """
    ph = _ph(project_ids)
    group_expr = _dedup_group_expr(dedup_mode)
    outer_prod = _dedup_producer_filter(dedup_mode, "hc")
    inner_prod = _dedup_producer_filter(dedup_mode, "hc2")
    picked_sql = f"""
        SELECT hc.project_id, hc.task_name, IFNULL(hc.video_seconds, 0) AS video_seconds,
               {ENV_FIELDS}
        FROM human_cases hc
        WHERE hc.project_id IN ({ph})
          AND hc.deleted_at IS NULL
          {outer_prod}
          AND hc.id IN (
              SELECT MAX(hc2.id)
              FROM human_cases hc2
              LEFT JOIN (
                  SELECT hcn.human_case_id, hcn.node_status
                  FROM human_case_nodes hcn
                  INNER JOIN (
                      SELECT human_case_id, MAX(id) AS max_id
                      FROM human_case_nodes
                      WHERE project_id IN ({ph}) AND node_name = 'human_case_inspect'
                      GROUP BY human_case_id
                  ) latest_insp ON latest_insp.max_id = hcn.id
              ) insp ON insp.human_case_id = hc2.id
              LEFT JOIN (
                  SELECT hcn.human_case_id, hcn.node_status
                  FROM human_case_nodes hcn
                  INNER JOIN (
                      SELECT human_case_id, MAX(id) AS max_id
                      FROM human_case_nodes
                      WHERE project_id IN ({ph}) AND node_name = 'human_case_sampling'
                      GROUP BY human_case_id
                  ) latest_samp ON latest_samp.max_id = hcn.id
              ) samp ON samp.human_case_id = hc2.id
              WHERE hc2.project_id IN ({ph})
                AND hc2.deleted_at IS NULL
                {inner_prod}
                AND (
                    samp.node_status = 3
                    OR (samp.node_status IS NULL AND insp.node_status = 3)
                )
              GROUP BY {group_expr}
          )
    """
    params = project_ids + project_ids + project_ids + project_ids
    return _execute_capped_rows(cur, picked_sql, params, packed=False,
                                task_cap_hours=task_cap_hours)


def query_labeling_inprogress_dedup(cur, project_ids, dedup_mode="task_producer",
                                    task_cap_hours=None):
    """标注中（去重版）：支持 task+producer 或 task 去重，并可按 task 时长封顶"""
    ph = _ph(project_ids)
    group_expr = _dedup_group_expr(dedup_mode)
    outer_prod = _dedup_producer_filter(dedup_mode, "hc")
    inner_prod = _dedup_producer_filter(dedup_mode, "hc2")
    picked_sql = f"""
        SELECT hc.project_id, hc.task_name, IFNULL(hc.video_seconds, 0) AS video_seconds,
               {ENV_FIELDS}
        FROM human_cases hc
        WHERE hc.project_id IN ({ph})
          AND hc.deleted_at IS NULL
          {outer_prod}
          AND hc.id IN (
              SELECT MAX(hc2.id)
              FROM human_cases hc2
              WHERE hc2.project_id IN ({ph})
                AND hc2.deleted_at IS NULL
                {inner_prod}
                AND EXISTS (
                    SELECT 1 FROM human_case_nodes hcn2
                    WHERE hcn2.human_case_id = hc2.id
                      AND hcn2.node_name IN ('semantics_labeling', 'pose_labeling')
                      AND hcn2.node_status = 1
                )
              GROUP BY {group_expr}
          )
    """
    params = project_ids + project_ids
    return _execute_capped_rows(cur, picked_sql, params, packed=False,
                                task_cap_hours=task_cap_hours)


def query_packaged_dedup(cur, project_ids, dedup_mode="task_producer",
                         task_cap_hours=None, packed_node=None):
    """打包成功（去重版）：支持 task+producer 或 task 去重，并可按 task 时长封顶"""
    ph = _ph(project_ids)
    group_expr = _dedup_group_expr(dedup_mode)
    outer_prod = _dedup_producer_filter(dedup_mode, "hc")
    inner_prod = _dedup_producer_filter(dedup_mode, "hc2")
    node_name = packed_node or "complete_job"
    picked_sql = f"""
        SELECT hc.project_id, hc.task_name,
               COALESCE(hc.delivery_video_seconds, 0) AS pack_seconds,
               {ENV_FIELDS}
        FROM human_cases hc
        WHERE hc.project_id IN ({ph})
          AND hc.deleted_at IS NULL
          {outer_prod}
          AND hc.id IN (
              SELECT MAX(hc2.id)
              FROM human_cases hc2
              INNER JOIN (
                  SELECT hcn.human_case_id
                  FROM human_case_nodes hcn
                  INNER JOIN (
                      SELECT human_case_id, MAX(id) AS max_id
                      FROM human_case_nodes
                      WHERE project_id IN ({ph}) AND node_name = %s
                      GROUP BY human_case_id
                  ) latest ON hcn.id = latest.max_id
                  WHERE hcn.node_status = 3
              ) packed ON hc2.id = packed.human_case_id
              WHERE hc2.project_id IN ({ph})
                AND hc2.deleted_at IS NULL
                {inner_prod}
              GROUP BY {group_expr}
          )
    """
    params = project_ids + project_ids + [node_name] + project_ids
    # cap_only 模式：延迟封顶，返回原始行供全局封顶
    defer_cap = (dedup_mode == "cap_only")
    return _execute_capped_rows(cur, picked_sql, params, packed=True,
                                task_cap_hours=task_cap_hours, defer_cap=defer_cap)


def query_qc_compat_stats(cur, project_ids):
    """
    质检口径兼容：
      - 若 case 存在 sampling 节点：以 sampling 为准
      - 若不存在 sampling 节点：以 human_case_inspect 为准
      - 特殊规则：sampling 为 1/2 且 inspect=3 时，按失败处理
    同时输出待质检/待抽检时长，便于监控积压。
    """
    ph = _ph(project_ids)
    cur.execute(f"""
        SELECT hc.project_id, {ENV_FIELDS},
               SUM(
                   CASE
                     WHEN samp.node_status = 3 THEN 1
                     WHEN samp.node_status IS NULL AND insp.node_status = 3 THEN 1
                     ELSE 0
                   END
               ) AS pass_cnt,
               SUM(
                   CASE
                     WHEN samp.node_status = 3 THEN IFNULL(hc.video_seconds, 0)
                     WHEN samp.node_status IS NULL AND insp.node_status = 3 THEN IFNULL(hc.video_seconds, 0)
                     ELSE 0
                   END
               ) / 3600.0 AS pass_hours,
               SUM(
                   CASE
                     WHEN samp.node_status = 4 THEN 1
                     WHEN samp.node_status IN (1, 2) AND insp.node_status = 3 THEN 1
                     WHEN samp.node_status IS NULL AND insp.node_status = 4 THEN 1
                     ELSE 0
                   END
               ) AS fail_cnt,
               SUM(
                   CASE
                     WHEN samp.node_status = 4 THEN IFNULL(hc.video_seconds, 0)
                     WHEN samp.node_status IN (1, 2) AND insp.node_status = 3 THEN IFNULL(hc.video_seconds, 0)
                     WHEN samp.node_status IS NULL AND insp.node_status = 4 THEN IFNULL(hc.video_seconds, 0)
                     ELSE 0
                   END
               ) / 3600.0 AS fail_hours,
               SUM(
                   CASE
                     WHEN samp.node_status IS NOT NULL AND samp.node_status IN (1, 2) THEN IFNULL(hc.video_seconds, 0)
                     WHEN samp.node_status IS NULL AND insp.node_status IN (1, 2) THEN IFNULL(hc.video_seconds, 0)
                     ELSE 0
                   END
               ) / 3600.0 AS pending_inspect_hours,
               SUM(
                   CASE
                     WHEN samp.node_status IN (1, 2) THEN IFNULL(hc.video_seconds, 0)
                     ELSE 0
                   END
               ) / 3600.0 AS pending_sampling_hours
        FROM human_cases hc
        LEFT JOIN (
            SELECT hcn.human_case_id, hcn.node_status
            FROM human_case_nodes hcn
            INNER JOIN (
                SELECT human_case_id, MAX(id) AS max_id
                FROM human_case_nodes
                WHERE project_id IN ({ph}) AND node_name = 'human_case_inspect'
                GROUP BY human_case_id
            ) latest_insp ON latest_insp.max_id = hcn.id
        ) insp ON insp.human_case_id = hc.id
        LEFT JOIN (
            SELECT hcn.human_case_id, hcn.node_status
            FROM human_case_nodes hcn
            INNER JOIN (
                SELECT human_case_id, MAX(id) AS max_id
                FROM human_case_nodes
                WHERE project_id IN ({ph}) AND node_name = 'human_case_sampling'
                GROUP BY human_case_id
            ) latest_samp ON latest_samp.max_id = hcn.id
        ) samp ON samp.human_case_id = hc.id
        WHERE hc.project_id IN ({ph})
          AND hc.deleted_at IS NULL
          AND (insp.node_status IS NOT NULL OR samp.node_status IS NOT NULL)
        GROUP BY hc.project_id, env_type_name, environment_num, env_num
    """, project_ids + project_ids + project_ids)
    return list(cur.fetchall())


DAILY_NODE_RULES = {
    "collect_done_hours": ("human_case_produce_complete", 3, "采集完成"),
    "qc_pass_hours": ("human_case_inspect", 3, "采集质检通过"),
    "label_done_hours": ("labeling_complete", 3, "标注完成"),
}


def _to_float_or_none(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def query_node_by_date(cur, project_ids, node_name, node_status):
    """Returns [{date, cnt, hours}] grouped by completion day for a given node/status."""
    ph = _ph(project_ids)
    cur.execute(f"""
        SELECT DATE(COALESCE(hcn.node_updated_at, hcn.updated_at)) AS day,
               COUNT(*) AS cnt,
               SUM(IFNULL(hc.video_seconds, 0)) / 3600.0 AS hours
        FROM human_cases hc
        INNER JOIN (
            SELECT human_case_id, MAX(id) AS max_id
            FROM human_case_nodes
            WHERE project_id IN ({ph}) AND node_name = %s
            GROUP BY human_case_id
        ) latest ON latest.human_case_id = hc.id
        INNER JOIN human_case_nodes hcn ON hcn.id = latest.max_id AND hcn.node_status = %s
        WHERE hc.project_id IN ({ph})
          AND hc.deleted_at IS NULL
        GROUP BY day
        ORDER BY day
    """, project_ids + [node_name, node_status] + project_ids)
    return [
        {"date": str(r["day"]), "cnt": int(r["cnt"]), "hours": float(r.get("hours") or 0)}
        for r in cur.fetchall() if r.get("day")
    ]


def query_qc_pass_by_date(cur, project_ids):
    """Returns [{date, cnt, hours}] for QC pass by day (sampling priority, inspect fallback)."""
    ph = _ph(project_ids)
    cur.execute(f"""
        SELECT DATE(COALESCE(hcn.node_updated_at, hcn.updated_at)) AS day,
               COUNT(*) AS cnt,
               SUM(IFNULL(hc.video_seconds, 0)) / 3600.0 AS hours
        FROM human_cases hc
        INNER JOIN (
            SELECT human_case_id, MAX(id) AS max_id
            FROM human_case_nodes
            WHERE project_id IN ({ph}) AND node_name = 'human_case_sampling'
            GROUP BY human_case_id
        ) latest ON latest.human_case_id = hc.id
        INNER JOIN human_case_nodes hcn ON hcn.id = latest.max_id AND hcn.node_status = 3
        WHERE hc.project_id IN ({ph})
          AND hc.deleted_at IS NULL
        GROUP BY day ORDER BY day
    """, project_ids * 2)
    combined = {}
    for r in cur.fetchall():
        if r.get("day"):
            d = str(r["day"])
            combined[d] = {"cnt": int(r["cnt"]), "hours": float(r.get("hours") or 0)}
    cur.execute(f"""
        SELECT DATE(COALESCE(hcn.node_updated_at, hcn.updated_at)) AS day,
               COUNT(*) AS cnt,
               SUM(IFNULL(hc.video_seconds, 0)) / 3600.0 AS hours
        FROM human_cases hc
        INNER JOIN (
            SELECT human_case_id, MAX(id) AS max_id
            FROM human_case_nodes
            WHERE project_id IN ({ph}) AND node_name = 'human_case_inspect'
            GROUP BY human_case_id
        ) latest ON latest.human_case_id = hc.id
        INNER JOIN human_case_nodes hcn ON hcn.id = latest.max_id AND hcn.node_status = 3
        WHERE hc.project_id IN ({ph})
          AND hc.deleted_at IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM human_case_nodes s
              WHERE s.project_id IN ({ph})
                AND s.node_name = 'human_case_sampling'
                AND s.human_case_id = hc.id
          )
        GROUP BY day ORDER BY day
    """, project_ids * 3)
    for r in cur.fetchall():
        if not r.get("day"):
            continue
        d = str(r["day"])
        cnt, hrs = int(r["cnt"]), float(r.get("hours") or 0)
        if d in combined:
            combined[d]["cnt"] += cnt
            combined[d]["hours"] += hrs
        else:
            combined[d] = {"cnt": cnt, "hours": hrs}
    return [{"date": d, "cnt": v["cnt"], "hours": v["hours"]} for d, v in sorted(combined.items())]


def _build_daily_arrays_from_cases(all_cases, lab_done_node=None):
    """Compute per-day arrays from in-memory new-DB cases."""
    _lab_node = lab_done_node or "labeling_complete"
    lab_by_date = defaultdict(lambda: {"cnt": 0, "hours": 0.0})
    collect_by_date = defaultdict(lambda: {"cnt": 0, "hours": 0.0})
    qc_by_date = defaultdict(lambda: {"cnt": 0, "hours": 0.0})
    for case in all_cases:
        nodes = case["nodes"]
        hrs = float(case.get("video_seconds") or 0.0) / 3600.0
        lab = nodes.get(_lab_node)
        if lab and lab.get("status") == 3 and lab.get("day"):
            lab_by_date[lab["day"]]["cnt"] += 1
            lab_by_date[lab["day"]]["hours"] += hrs
        collect = nodes.get("human_case_produce_complete")
        if collect and collect.get("status") == 3 and collect.get("day"):
            collect_by_date[collect["day"]]["cnt"] += 1
            collect_by_date[collect["day"]]["hours"] += hrs
        samp = nodes.get("human_case_sampling")
        insp = nodes.get("human_case_inspect")
        if samp and samp.get("status") == 3 and samp.get("day"):
            qc_by_date[samp["day"]]["cnt"] += 1
            qc_by_date[samp["day"]]["hours"] += hrs
        elif (not samp or not samp.get("status")) and insp and insp.get("status") == 3 and insp.get("day"):
            qc_by_date[insp["day"]]["cnt"] += 1
            qc_by_date[insp["day"]]["hours"] += hrs
    to_arr = lambda d: [{"date": k, "cnt": v["cnt"], "hours": v["hours"]} for k, v in sorted(d.items())]
    return to_arr(lab_by_date), to_arr(collect_by_date), to_arr(qc_by_date)


def _merge_daily_arrays(arrays_list):
    merged = defaultdict(lambda: {"cnt": 0, "hours": 0.0})
    for arr in arrays_list:
        for item in (arr or []):
            d = item["date"]
            merged[d]["cnt"] += item["cnt"]
            merged[d]["hours"] += item["hours"]
    return [{"date": d, "cnt": v["cnt"], "hours": v["hours"]} for d, v in sorted(merged.items())]


def resolve_daily_goals(project_config, day_str):
    """
    读取项目今日目标：
    1) project.daily_goals（默认日目标）
    2) project.daily_targets 中同日期配置（仅覆盖同名键）
    """
    merged = {}
    base = project_config.get("daily_goals")
    if isinstance(base, dict):
        merged.update(base)

    for item in project_config.get("daily_targets", []):
        if isinstance(item, dict) and str(item.get("date")) == day_str:
            merged.update(item)
            break

    out = {}
    for metric_key in DAILY_NODE_RULES:
        out[metric_key] = _to_float_or_none(merged.get(metric_key))
    return out


def query_daily_actuals(cur, project_ids, day_str, dedup_by_task_producer=True):
    """
    统计某天完成量（小时）：
      - 采集完成：human_case_produce_complete status=3
      - 采集质检通过（兼容口径）：
          sampling=3
          或（无 sampling 且 inspect=3）
      - 标注完成：labeling_complete status=3
    dedup_by_task_producer=True 时去重口径：
      - 同一 case + node_name 当天多条记录，按 node MAX(id)
      - 业务去重按 task_name + producer；producer 为空时回退到 case id（避免误合并）
    """
    if not dedup_by_task_producer:
        return query_daily_actuals_case_level(cur, project_ids, day_str)

    ph = _ph(project_ids)

    def node_hours_dedup(node_name):
        cur.execute(f"""
            SELECT SUM(IFNULL(hc.video_seconds, 0)) / 3600.0 AS hours
            FROM human_cases hc
            WHERE hc.project_id IN ({ph})
          AND hc.deleted_at IS NULL
              AND hc.id IN (
                SELECT MAX(hc2.id)
                FROM human_cases hc2
                INNER JOIN (
                    SELECT hcn.human_case_id
                    FROM human_case_nodes hcn
                    INNER JOIN (
                        SELECT human_case_id, MAX(id) AS max_id
                        FROM human_case_nodes
                        WHERE project_id IN ({ph})
                          AND node_name = %s
                          AND DATE(COALESCE(node_updated_at, updated_at)) = %s
                        GROUP BY human_case_id
                    ) latest ON latest.max_id = hcn.id
                    WHERE hcn.node_status = 3
                ) t ON t.human_case_id = hc2.id
                WHERE hc2.project_id IN ({ph})
                AND hc2.deleted_at IS NULL
                GROUP BY hc2.task_name,
                         COALESCE(NULLIF(hc2.producer, ''), CONCAT('__id__', hc2.id))
              )
        """, project_ids + project_ids + [node_name, day_str] + project_ids)
        rows = list(cur.fetchall())
        r = rows[0] if rows else {}
        return float(r.get("hours") or 0.0)

    collect_done_h = node_hours_dedup("human_case_produce_complete")
    label_done_h = node_hours_dedup("labeling_complete")

    # 采集质检通过（按 sampling 兼容口径）+ task_name/producer 去重
    cur.execute(f"""
        SELECT SUM(IFNULL(hc.video_seconds, 0)) / 3600.0 AS hours
        FROM human_cases hc
        WHERE hc.project_id IN ({ph})
          AND hc.deleted_at IS NULL
          AND hc.id IN (
            SELECT MAX(hc2.id)
            FROM human_cases hc2
            INNER JOIN (
                -- sampling 成功（当日）
                SELECT hcn.human_case_id
                FROM human_case_nodes hcn
                INNER JOIN (
                    SELECT human_case_id, MAX(id) AS max_id
                    FROM human_case_nodes
                    WHERE project_id IN ({ph})
                      AND node_name = 'human_case_sampling'
                      AND DATE(COALESCE(node_updated_at, updated_at)) = %s
                    GROUP BY human_case_id
                ) latest_samp ON latest_samp.max_id = hcn.id
                WHERE hcn.node_status = 3

                UNION ALL

                -- 无 sampling 时 inspect 成功（当日）
                SELECT hcn.human_case_id
                FROM human_case_nodes hcn
                INNER JOIN (
                    SELECT human_case_id, MAX(id) AS max_id
                    FROM human_case_nodes
                    WHERE project_id IN ({ph})
                      AND node_name = 'human_case_inspect'
                      AND DATE(COALESCE(node_updated_at, updated_at)) = %s
                    GROUP BY human_case_id
                ) latest_insp ON latest_insp.max_id = hcn.id
                WHERE hcn.node_status = 3
                  AND NOT EXISTS (
                      SELECT 1
                      FROM human_case_nodes s
                      WHERE s.project_id IN ({ph})
                        AND s.node_name = 'human_case_sampling'
                        AND s.human_case_id = hcn.human_case_id
                  )
            ) q ON q.human_case_id = hc2.id
            WHERE hc2.project_id IN ({ph})
                AND hc2.deleted_at IS NULL
            GROUP BY hc2.task_name,
                     COALESCE(NULLIF(hc2.producer, ''), CONCAT('__id__', hc2.id))
          )
    """, project_ids + project_ids + [day_str] + project_ids + [day_str] + project_ids + project_ids)
    rows = list(cur.fetchall())
    qc_pass_h = float((rows[0] if rows else {}).get("hours") or 0.0)

    return {
        "collect_done_hours": collect_done_h,
        "qc_pass_hours": qc_pass_h,
        "label_done_hours": label_done_h,
    }


def query_daily_actuals_case_level(cur, project_ids, day_str):
    """
    统计某天完成量（小时）：
      - 仅做 case 级去重（同一 case + node_name 当天按 MAX(id)）
      - 不做 task_name + producer 去重
      - 质检通过口径仍为 sampling 优先兼容口径
    """
    ph = _ph(project_ids)

    def node_hours(node_name):
        cur.execute(f"""
            SELECT SUM(IFNULL(hc.video_seconds, 0)) / 3600.0 AS hours
            FROM human_cases hc
            INNER JOIN (
                SELECT hcn.human_case_id
                FROM human_case_nodes hcn
                INNER JOIN (
                    SELECT human_case_id, MAX(id) AS max_id
                    FROM human_case_nodes
                    WHERE project_id IN ({ph})
                      AND node_name = %s
                      AND DATE(COALESCE(node_updated_at, updated_at)) = %s
                    GROUP BY human_case_id
                ) latest ON latest.max_id = hcn.id
                WHERE hcn.node_status = 3
            ) t ON t.human_case_id = hc.id
            WHERE hc.project_id IN ({ph})
          AND hc.deleted_at IS NULL
        """, project_ids + [node_name, day_str] + project_ids)
        rows = list(cur.fetchall())
        r = rows[0] if rows else {}
        return float(r.get("hours") or 0.0)

    collect_done_h = node_hours("human_case_produce_complete")
    label_done_h = node_hours("labeling_complete")

    # sampling 成功
    cur.execute(f"""
        SELECT SUM(IFNULL(hc.video_seconds, 0)) / 3600.0 AS hours
        FROM human_cases hc
        INNER JOIN (
            SELECT hcn.human_case_id
            FROM human_case_nodes hcn
            INNER JOIN (
                SELECT human_case_id, MAX(id) AS max_id
                FROM human_case_nodes
                WHERE project_id IN ({ph})
                  AND node_name = 'human_case_sampling'
                  AND DATE(COALESCE(node_updated_at, updated_at)) = %s
                GROUP BY human_case_id
            ) latest_samp ON latest_samp.max_id = hcn.id
            WHERE hcn.node_status = 3
        ) s ON s.human_case_id = hc.id
        WHERE hc.project_id IN ({ph})
          AND hc.deleted_at IS NULL
    """, project_ids + [day_str] + project_ids)
    rows = list(cur.fetchall())
    sampling_pass_h = float((rows[0] if rows else {}).get("hours") or 0.0)

    # 无 sampling 时 inspect 成功
    cur.execute(f"""
        SELECT SUM(IFNULL(hc.video_seconds, 0)) / 3600.0 AS hours
        FROM human_cases hc
        INNER JOIN (
            SELECT hcn.human_case_id
            FROM human_case_nodes hcn
            INNER JOIN (
                SELECT human_case_id, MAX(id) AS max_id
                FROM human_case_nodes
                WHERE project_id IN ({ph})
                  AND node_name = 'human_case_inspect'
                  AND DATE(COALESCE(node_updated_at, updated_at)) = %s
                GROUP BY human_case_id
            ) latest_insp ON latest_insp.max_id = hcn.id
            WHERE hcn.node_status = 3
              AND NOT EXISTS (
                  SELECT 1
                  FROM human_case_nodes s
                  WHERE s.project_id IN ({ph})
                    AND s.node_name = 'human_case_sampling'
                    AND s.human_case_id = hcn.human_case_id
              )
        ) i ON i.human_case_id = hc.id
        WHERE hc.project_id IN ({ph})
          AND hc.deleted_at IS NULL
    """, project_ids + [day_str] + project_ids + project_ids)
    rows = list(cur.fetchall())
    inspect_pass_without_sampling_h = float((rows[0] if rows else {}).get("hours") or 0.0)

    return {
        "collect_done_hours": collect_done_h,
        "qc_pass_hours": sampling_pass_h + inspect_pass_without_sampling_h,
        "label_done_hours": label_done_h,
    }


# ── 聚合：已知环境 vs 未知环境分开 ───────────────────────────────────────────

def aggregate(rows, scene_mapping, allow_all=False):
    """
    返回:
      known   = {scene_name: {"hours": float, "cnt": int}}   # 只含已映射环境
      unknown = {env_key:    {"hours": float, "cnt": int}}   # 未识别，不归入任何环境
    """
    known   = defaultdict(lambda: {"hours": 0.0, "cnt": 0})
    unknown = defaultdict(lambda: {"hours": 0.0, "cnt": 0})
    for row in rows:
        env_key = parse_env_key(row)
        scene   = resolve_scene(env_key, scene_mapping, allow_all=allow_all)
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


def aggregate_qc_compat(rows, scene_mapping, allow_all=False):
    known = defaultdict(lambda: {
        "pass": 0,
        "fail": 0,
        "pass_h": 0.0,
        "fail_h": 0.0,
        "pending_cnt": 0,
        "pending_h": 0.0,
        "pending_inspect_h": 0.0,
        "pending_sampling_h": 0.0,
        "pending_sampling_cnt": 0,
    })
    unknown = defaultdict(lambda: {
        "pass": 0,
        "fail": 0,
        "pass_h": 0.0,
        "fail_h": 0.0,
        "pending_cnt": 0,
        "pending_h": 0.0,
        "pending_inspect_h": 0.0,
        "pending_sampling_h": 0.0,
        "pending_sampling_cnt": 0,
    })

    for row in rows:
        env_key = parse_env_key(row)
        scene = resolve_scene(env_key, scene_mapping, allow_all=allow_all)
        target = known[scene] if scene is not None else unknown[env_key or "(空)"]

        pass_cnt = int(row.get("pass_cnt") or 0)
        fail_cnt = int(row.get("fail_cnt") or 0)
        pass_h = float(row.get("pass_hours") or 0.0)
        fail_h = float(row.get("fail_hours") or 0.0)
        pending_inspect_h = float(row.get("pending_inspect_hours") or 0.0)
        pending_sampling_h = float(row.get("pending_sampling_hours") or 0.0)

        target["pass"] += pass_cnt
        target["fail"] += fail_cnt
        target["pass_h"] += pass_h
        target["fail_h"] += fail_h
        target["pending_inspect_h"] += pending_inspect_h
        target["pending_sampling_h"] += pending_sampling_h
        target["pending_h"] += pending_inspect_h
        
        # 待质检条数：有待质检时长的才计数
        if pending_inspect_h > 0:
            target["pending_cnt"] += 1
        # 待抽检条数：有待抽检时长的才计数
        if pending_sampling_h > 0:
            target["pending_sampling_cnt"] += 1

    return dict(known), dict(unknown)


def h(val):
    return f"{val:.1f}h" if val else "0.0h"


def log(msg):
    print(f"  {msg}", file=sys.stderr)


# ── 主查询 ────────────────────────────────────────────────────────────────────

def fetch_projects_by_tag(tag_name):
    """按项目标签拉取关联项目（仅保留有效关联+未删除项目）"""
    conn = pymysql.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT p.id, p.name
                FROM project_tag_project ptp
                INNER JOIN project_tag t ON t.id = ptp.tag_id
                INNER JOIN projects p ON p.id = ptp.project_id
                WHERE LOWER(t.tag_name) = LOWER(%s)
                  AND ptp.deleted_at IS NULL
                  AND p.is_deleted = 0
                ORDER BY p.name
            """, [tag_name])
            return list(cur.fetchall())
    finally:
        conn.close()

def fetch_projects_by_tag_newdb(tag_name):
    """按项目标签拉取新库关联项目（仅保留有效关联+未删除项目）"""
    conn = pymysql.connect(**NEW_DB_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT p.uuid AS id, p.name
                FROM project_tag_project ptp
                INNER JOIN project_tag t ON t.id = ptp.tag_id
                INNER JOIN project p ON p.id = ptp.project_id
                WHERE LOWER(t.tag_name) = LOWER(%s)
                  AND ptp.deleted_at IS NULL
                  AND p.deleted_at IS NULL
                ORDER BY p.name
            """, [tag_name])
            return list(cur.fetchall())
    finally:
        conn.close()


def fetch_projects_by_tag_all_sources(tag_name):
    asset_projects = [
        {"id": r["id"], "name": r.get("name", r["id"]), "source": "asset"}
        for r in fetch_projects_by_tag(tag_name)
    ]
    newdb_projects = [
        {"id": r["id"], "name": r.get("name", r["id"]), "source": "newdb"}
        for r in fetch_projects_by_tag_newdb(tag_name)
    ]
    return asset_projects + newdb_projects


def run_project_asset(project_config, scene_mapping, project_entries=None):
    project_entries = project_entries or project_config["query_projects"]
    project_ids = [p["id"] for p in project_entries]
    project_name_map = {p["id"]: p.get("name", p["id"]) for p in project_entries}
    dedup = project_config.get("dedup_by_producer_scene", False)
    dedup_mode = project_config.get("dedup_mode", "task_producer")
    dedup_task_cap_hours = _to_float_or_none(project_config.get("dedup_task_cap_hours"))
    packed_task_cap_hours = _to_float_or_none(project_config.get("packed_task_cap_hours"))
    packed_dedup_task_cap_hours = _to_float_or_none(
        project_config.get("packed_dedup_task_cap_hours")
    )
    packed_dedup_enabled = bool(project_config.get("packed_dedup_enabled", True))
    packed_node = project_config.get("packed_node")  # 自定义打包节点

    conn = pymysql.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            # 按中国时区做"当日"统计，避免跨时区日界线偏移
            cur.execute("SET time_zone = '+08:00'")
            log("采集质检成功（兼容 sampling）...")
            qc_rows = query_qc_compat_stats(cur, project_ids)
            qc_pass_rows = []
            for row in qc_rows:
                qc_pass_rows.append({
                    "project_id": row.get("project_id"),
                    "env_type_name": row.get("env_type_name"),
                    "environment_num": row.get("environment_num"),
                    "env_num": row.get("env_num"),
                    "cnt": int(row.get("pass_cnt") or 0),
                    "hours": float(row.get("pass_hours") or 0.0),
                })
            qc_pass_k, qc_pass_u = aggregate(qc_pass_rows, scene_mapping)
            qc_known, qc_unknown = aggregate_qc_compat(qc_rows, scene_mapping)

            # 待质检时长按采集项目分布（仅 pending_inspect_hours）
            pending_by_project = defaultdict(float)
            for row in qc_rows:
                project_id = row.get("project_id")
                pending_by_project[project_id] += float(row.get("pending_inspect_hours") or 0.0)
            qc_pending_project = [
                {
                    "project_id": pid,
                    "project_name": project_name_map.get(pid, pid),
                    "pending_inspect_h": hours,
                }
                for pid, hours in pending_by_project.items()
            ]
            qc_pending_project.sort(key=lambda x: x["pending_inspect_h"], reverse=True)
            log("语义标注中...")
            sem_ing_k,  sem_ing_u  = aggregate(query_node(cur, project_ids, "semantics_labeling", 1), scene_mapping)
            log("语义标注完成...")
            sem_done_k, sem_done_u = aggregate(query_node(cur, project_ids, "semantics_labeling", 3), scene_mapping)
            log("手势标注中...")
            pose_ing_k, pose_ing_u = aggregate(query_node(cur, project_ids, "pose_labeling", 1), scene_mapping)
            log("手势标注完成...")
            pose_done_k, pose_done_u = aggregate(query_node(cur, project_ids, "pose_labeling", 3), scene_mapping)
            log("标注中（去重）...")
            lab_ing_k,  lab_ing_u  = aggregate(query_labeling_inprogress(cur, project_ids), scene_mapping)
            log("标注完成...")
            lab_done_k, lab_done_u = aggregate(query_node(cur, project_ids, "labeling_complete", 3), scene_mapping)
            log("打包成功...")
            packed_k,   packed_u   = aggregate(
                query_packaged(cur, project_ids, task_cap_hours=packed_task_cap_hours, packed_node=packed_node),
                scene_mapping,
            )
            today_str = datetime.now().strftime("%Y-%m-%d")
            log(f"今日目标达成（{today_str}）...")
            daily_actual_dedup = bool(project_config.get("daily_actual_dedup", False))
            daily_actual = query_daily_actuals(
                cur,
                project_ids,
                today_str,
                dedup_by_task_producer=daily_actual_dedup,
            )
            daily_target = resolve_daily_goals(project_config, today_str)
            log("每日标注完成趋势...")
            lab_done_daily = query_node_by_date(cur, project_ids, "labeling_complete", 3)
            log("每日采集完成趋势...")
            collect_done_daily = query_node_by_date(cur, project_ids, "human_case_produce_complete", 3)
            log("每日质检通过趋势...")
            qc_pass_daily = query_qc_pass_by_date(cur, project_ids)

            # 去重版（仅 dedup_by_producer_scene 项目）
            qc_pass_dedup_k = qc_pass_dedup_u = None
            sem_ing_dedup_k = sem_ing_dedup_u = None
            pose_ing_dedup_k = pose_ing_dedup_u = None
            lab_ing_dedup_k = lab_ing_dedup_u = None
            lab_done_dedup_k = lab_done_dedup_u = None
            packed_dedup_k  = packed_dedup_u  = None
            if dedup:
                log("采集质检成功（去重，兼容 sampling）...")
                qc_pass_dedup_k, qc_pass_dedup_u = aggregate(
                    query_qc_pass_dedup_compat(
                        cur,
                        project_ids,
                        dedup_mode=dedup_mode,
                        task_cap_hours=dedup_task_cap_hours,
                    ),
                    scene_mapping,
                )
                log("语义标注中（去重）...")
                sem_ing_dedup_k, sem_ing_dedup_u = aggregate(
                    query_node_dedup(
                        cur,
                        project_ids,
                        "semantics_labeling",
                        1,
                        dedup_mode=dedup_mode,
                        task_cap_hours=dedup_task_cap_hours,
                    ),
                    scene_mapping,
                )
                log("手势标注中（去重）...")
                pose_ing_dedup_k, pose_ing_dedup_u = aggregate(
                    query_node_dedup(
                        cur,
                        project_ids,
                        "pose_labeling",
                        1,
                        dedup_mode=dedup_mode,
                        task_cap_hours=dedup_task_cap_hours,
                    ),
                    scene_mapping,
                )
                log("标注中（去重，去重版）...")
                lab_ing_dedup_k, lab_ing_dedup_u = aggregate(
                    query_labeling_inprogress_dedup(
                        cur,
                        project_ids,
                        dedup_mode=dedup_mode,
                        task_cap_hours=dedup_task_cap_hours,
                    ),
                    scene_mapping,
                )
                log("标注完成（去重）...")
                lab_done_dedup_k, lab_done_dedup_u = aggregate(
                    query_node_dedup(
                        cur,
                        project_ids,
                        "labeling_complete",
                        3,
                        dedup_mode=dedup_mode,
                        task_cap_hours=dedup_task_cap_hours,
                    ),
                    scene_mapping,
                )
                if packed_dedup_enabled:
                    log("打包成功（去重）...")
                    packed_dedup_rows = query_packaged_dedup(
                        cur,
                        project_ids,
                        dedup_mode=dedup_mode,
                        task_cap_hours=(
                            packed_dedup_task_cap_hours
                            if packed_dedup_task_cap_hours is not None
                            else dedup_task_cap_hours
                        ),
                        packed_node=packed_node,
                    )
                    packed_dedup_k, packed_dedup_u = aggregate(
                        packed_dedup_rows,
                        scene_mapping,
                    )
    finally:
        conn.close()

    # 合并所有未知 env_key（跨指标）
    all_unknown = defaultdict(lambda: {"hours": 0.0, "cnt": 0})
    for uk in (qc_pass_u, sem_ing_u, sem_done_u, pose_ing_u, pose_done_u, lab_ing_u, lab_done_u, packed_u):
        for key, v in uk.items():
            all_unknown[key]["hours"] += v["hours"]
            all_unknown[key]["cnt"]   += v["cnt"]
    for key, v in qc_unknown.items():
        all_unknown[key]["cnt"] = max(all_unknown[key]["cnt"],
                                      v["pass"] + v["fail"])

    return {
        "known": {
            "qc_pass":        qc_pass_k,
            "sem_ing":        sem_ing_k,
            "sem_done":       sem_done_k,
            "pose_ing":       pose_ing_k,
            "pose_done":      pose_done_k,
            "lab_ing":        lab_ing_k,
            "lab_done":       lab_done_k,
            "packed":         packed_k,
            "qc_scene":       dict(qc_known),
            "lab_done_daily":     lab_done_daily,
            "collect_done_daily": collect_done_daily,
            "qc_pass_daily":      qc_pass_daily,
            "pending_by_project": qc_pending_project,
            # 去重版（None 表示未启用）
            "qc_pass_dedup":  qc_pass_dedup_k,
            "sem_ing_dedup":  sem_ing_dedup_k,
            "pose_ing_dedup": pose_ing_dedup_k,
            "lab_ing_dedup":  lab_ing_dedup_k,
            "lab_done_dedup": lab_done_dedup_k,
            "packed_dedup":   packed_dedup_k,
        },
        "unknown": dict(all_unknown),
        "dedup": dedup,
        "daily": {
            "date": today_str,
            "actual": daily_actual,
            "target": daily_target,
        },
        "_packed_dedup_rows": packed_dedup_rows if (dedup and packed_dedup_enabled) else [],
    }


def _empty_result(project_config):
    today_str = datetime.now().strftime("%Y-%m-%d")
    daily_target = resolve_daily_goals(project_config, today_str)
    return {
        "known": {
            "qc_pass": {},
            "sem_ing": {},
            "sem_done": {},
            "pose_ing": {},
            "pose_done": {},
            "lab_ing": {},
            "lab_done": {},
            "packed": {},
            "qc_scene": {},
            "lab_done_daily": [],
            "collect_done_daily": [],
            "qc_pass_daily": [],
            "pending_by_project": [],
            "qc_pass_dedup": None,
            "sem_ing_dedup": None,
            "pose_ing_dedup": None,
            "lab_ing_dedup": None,
            "lab_done_dedup": None,
            "packed_dedup": None,
            "deliver_inspect": None,
            "deliver_inspect_dedup": None,
        },
        "unknown": {},
        "dedup": bool(project_config.get("dedup_by_producer_scene", False)),
        "daily": {
            "date": today_str,
            "actual": {
                "collect_done_hours": 0.0,
                "qc_pass_hours": 0.0,
                "label_done_hours": 0.0,
            },
            "target": daily_target,
        },
        "linked_projects": [],
    }


def _merge_hours_maps(maps):
    merged = defaultdict(lambda: {"hours": 0.0, "cnt": 0})
    for metric_map in maps:
        if not metric_map:
            continue
        for key, value in metric_map.items():
            merged[key]["hours"] += float(value.get("hours") or 0.0)
            merged[key]["cnt"] += int(value.get("cnt") or 0)
    return dict(merged)


def _merge_qc_scene_maps(maps):
    merged = defaultdict(
        lambda: {
            "pass": 0,
            "fail": 0,
            "pass_h": 0.0,
            "fail_h": 0.0,
            "pending_cnt": 0,
            "pending_sampling_cnt": 0,
            "pending_h": 0.0,
            "pending_inspect_h": 0.0,
            "pending_sampling_h": 0.0,
        }
    )
    for qc_map in maps:
        if not qc_map:
            continue
        for key, value in qc_map.items():
            merged[key]["pass"] += int(value.get("pass") or 0)
            merged[key]["fail"] += int(value.get("fail") or 0)
            merged[key]["pass_h"] += float(value.get("pass_h") or 0.0)
            merged[key]["fail_h"] += float(value.get("fail_h") or 0.0)
            merged[key]["pending_cnt"] += int(value.get("pending_cnt") or 0)
            merged[key]["pending_sampling_cnt"] += int(value.get("pending_sampling_cnt") or 0)
            merged[key]["pending_h"] += float(value.get("pending_h") or 0.0)
            merged[key]["pending_inspect_h"] += float(value.get("pending_inspect_h") or 0.0)
            merged[key]["pending_sampling_h"] += float(value.get("pending_sampling_h") or 0.0)
    return dict(merged)


def _merge_unknown_maps(maps):
    merged = defaultdict(lambda: {"hours": 0.0, "cnt": 0})
    for unknown_map in maps:
        if not unknown_map:
            continue
        for key, value in unknown_map.items():
            merged[key]["hours"] += float(value.get("hours") or 0.0)
            merged[key]["cnt"] += int(value.get("cnt") or 0)
    return dict(merged)


def _merge_pending_projects(project_lists):
    merged = defaultdict(float)
    name_map = {}
    for items in project_lists:
        for item in items or []:
            key = item.get("project_id")
            if key is None:
                continue
            merged[key] += float(item.get("pending_inspect_h") or 0.0)
            name_map[key] = item.get("project_name", key)
    rows = [
        {
            "project_id": key,
            "project_name": name_map.get(key, key),
            "pending_inspect_h": hours,
        }
        for key, hours in merged.items()
    ]
    rows.sort(key=lambda x: x["pending_inspect_h"], reverse=True)
    return rows


def _merge_daily(results):
    if not results:
        return {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "actual": {
                "collect_done_hours": 0.0,
                "qc_pass_hours": 0.0,
                "label_done_hours": 0.0,
            },
            "target": {},
        }
    first = results[0]["daily"]
    merged_actual = {
        "collect_done_hours": 0.0,
        "qc_pass_hours": 0.0,
        "label_done_hours": 0.0,
    }
    for result in results:
        actual = result.get("daily", {}).get("actual", {})
        for key in merged_actual:
            merged_actual[key] += float(actual.get(key) or 0.0)
    return {
        "date": first.get("date", datetime.now().strftime("%Y-%m-%d")),
        "actual": merged_actual,
        "target": first.get("target", {}),
    }


def _project_link_rows(project_entries):
    return [
        {
            "id": item["id"],
            "name": item.get("name", item["id"]),
            "source": item.get("source", "asset"),
        }
        for item in project_entries
    ]


def _latest_node_status(node_map, *names):
    for name in names:
        if name in node_map:
            return node_map[name].get("status")
    return None


def _latest_node_day(node_map, *names):
    for name in names:
        if name in node_map:
            return node_map[name].get("day")
    return None


def _base_metric_row(case):
    return {
        "project_id": case["project_id"],
        "project_name": case["project_name"],
        "env_type_name": case.get("env_type_name", ""),
        "environment_num": case.get("environment_num", ""),
        "env_num": case.get("env_num", ""),
        "task_name": case.get("task_name", ""),
        "producer": case.get("producer", ""),
        "case_id": int(case["id"]),
    }


def _apply_task_cap_rows(rows, task_cap_hours):
    if not rows or not task_cap_hours or task_cap_hours <= 0:
        return [dict(row) for row in rows]

    cap = float(task_cap_hours)
    accumulated = defaultdict(float)
    capped = []
    for row in rows:
        task_key = row.get("task_name") or ""
        h = float(row.get("hours") or 0.0)
        already = accumulated[task_key]
        if already >= cap:
            continue
        take = min(h, cap - already)
        accumulated[task_key] += take
        new_row = dict(row)
        new_row["hours"] = take
        capped.append(new_row)
    return capped


def _dedup_metric_rows(rows, dedup_mode="task_producer", task_cap_hours=None):
    if not rows:
        return []

    # cap_only 和 task_producer_global_cap 模式：不去重、不封顶，直接返回原始行供全局处理
    if dedup_mode in ("cap_only", "task_producer_global_cap"):
        return [dict(row) for row in rows]

    picked = {}
    for row in rows:
        if dedup_mode == "task":
            key = row.get("task_name") or ""
        else:
            producer = (row.get("producer") or "").strip()
            if not producer:
                continue
            key = (row.get("task_name") or "", producer)

        prev = picked.get(key)
        if prev is None or int(row.get("case_id") or 0) > int(prev.get("case_id") or 0):
            picked[key] = dict(row)

    return _apply_task_cap_rows(list(picked.values()), task_cap_hours)


def _dedup_daily_cases(cases, dedup_by_task_producer):
    if not dedup_by_task_producer:
        return list(cases)

    picked = {}
    for case in cases:
        producer = (case.get("producer") or "").strip()
        key = (
            case["project_id"],
            case.get("task_name") or "",
            producer or f"__id__{case['id']}",
        )
        prev = picked.get(key)
        if prev is None or int(case["id"]) > int(prev["id"]):
            picked[key] = case
    return list(picked.values())


def run_project_newdb(project_config, scene_mapping, project_entries):
    if not project_entries:
        return _empty_result(project_config)

    today_str = datetime.now().strftime("%Y-%m-%d")
    daily_target = resolve_daily_goals(project_config, today_str)
    dedup = bool(project_config.get("dedup_by_producer_scene", False))
    dedup_mode = project_config.get("dedup_mode", "task_producer")
    dedup_task_cap_hours = _to_float_or_none(project_config.get("dedup_task_cap_hours"))
    packed_task_cap_hours = _to_float_or_none(project_config.get("packed_task_cap_hours"))
    packed_dedup_task_cap_hours = _to_float_or_none(
        project_config.get("packed_dedup_task_cap_hours")
    )
    packed_dedup_enabled = bool(project_config.get("packed_dedup_enabled", True))
    packed_node = project_config.get("packed_node")  # 自定义打包节点，None 则用默认
    lab_done_node = project_config.get("lab_done_node")  # 自定义标注完成节点，None 则用 labeling_complete
    has_deliver_inspect = bool(project_config.get("show_deliver_inspect", False))
    allow_all_scenes = bool(project_config.get("allow_all_scenes", False))

    node_names = [
        "human_case_sampling",
        "human_case_inspect",
        "semantic_labeling",
        "semantics_labeling",
        "pose_labeling",
        "labeling_complete",
        "delivery_packaging",
        "complete_job",
        "human_case_produce_complete",
    ]
    if has_deliver_inspect:
        node_names.append("deliver_inspect")
    if packed_node and packed_node not in node_names:
        node_names.append(packed_node)
    node_placeholders = ",".join(["%s"] * len(node_names))

    all_cases = []
    linked_projects = _project_link_rows(project_entries)

    conn = pymysql.connect(**NEW_DB_CONFIG)
    try:
        with conn.cursor() as cur:
            for item in project_entries:
                project_uuid = item["id"]
                project_name = item.get("name", project_uuid)
                cur.execute(
                    """
                    SELECT
                        hc.id,
                        hc.uuid,
                        hc.project_uuid,
                        hc.name,
                        hc.producer,
                        COALESCE(
                            NULLIF(JSON_UNQUOTE(JSON_EXTRACT(ctx.value, '$.task_name')), ''),
                            NULLIF(slog.task_name, ''),
                            CONCAT('task_id:', CAST(hc.task_id AS CHAR))
                        ) AS task_name,
                        COALESCE(
                            CAST(JSON_EXTRACT(prod.value, '$.data_info.duration') AS DECIMAL(18, 6)),
                            IFNULL(slog.video_seconds, 0),
                            0
                        ) AS video_seconds,
                        COALESCE(
                            CAST(JSON_EXTRACT(dlv.value, '$.delivery_video_seconds') AS DECIMAL(18, 6)),
                            0
                        ) AS delivery_video_seconds,
                        COALESCE(
                            JSON_UNQUOTE(JSON_EXTRACT(ctx.value, '$.env_type_name')),
                            ''
                        ) AS env_type_name,
                        COALESCE(
                            JSON_UNQUOTE(JSON_EXTRACT(ctx.value, '$.environment_num')),
                            JSON_UNQUOTE(JSON_EXTRACT(ctx.value, '$.env_num')),
                            ''
                        ) AS environment_num,
                        COALESCE(
                            JSON_UNQUOTE(JSON_EXTRACT(ctx.value, '$.env_num')),
                            JSON_UNQUOTE(JSON_EXTRACT(ctx.value, '$.environment_num')),
                            ''
                        ) AS env_num
                    FROM human_case hc
                    LEFT JOIN (
                        SELECT t1.human_case_id, t1.value
                        FROM human_case_tag t1
                        INNER JOIN (
                            SELECT human_case_id, MAX(id) AS max_id
                            FROM human_case_tag
                            WHERE type = 'produce_tags'
                            GROUP BY human_case_id
                        ) tp ON tp.max_id = t1.id
                    ) prod ON prod.human_case_id = hc.id
                    LEFT JOIN (
                        SELECT t1.human_case_id, t1.value
                        FROM human_case_tag t1
                        INNER JOIN (
                            SELECT human_case_id, MAX(id) AS max_id
                            FROM human_case_tag
                            WHERE type = 'context_tags'
                            GROUP BY human_case_id
                        ) tc ON tc.max_id = t1.id
                    ) ctx ON ctx.human_case_id = hc.id
                    LEFT JOIN (
                        SELECT t1.human_case_id, t1.value
                        FROM human_case_tag t1
                        INNER JOIN (
                            SELECT human_case_id, MAX(id) AS max_id
                            FROM human_case_tag
                            WHERE type = 'delivery_tags'
                            GROUP BY human_case_id
                        ) td ON td.max_id = t1.id
                    ) dlv ON dlv.human_case_id = hc.id
                    LEFT JOIN (
                        SELECT l1.human_case_uuid, l1.task_name, l1.video_seconds
                        FROM human_case_sample_log l1
                        INNER JOIN (
                            SELECT human_case_uuid, MAX(id) AS max_id
                            FROM human_case_sample_log
                            WHERE project_uuid = %s
                            GROUP BY human_case_uuid
                        ) ls ON ls.max_id = l1.id
                    ) slog ON slog.human_case_uuid = hc.uuid
                    WHERE hc.project_uuid = %s
                      AND hc.deleted_at IS NULL
                    """,
                    [project_uuid, project_uuid],
                )
                case_rows = list(cur.fetchall())
                if not case_rows:
                    continue

                cur.execute(
                    f"""
                    SELECT human_case_id, node_name, node_status,
                           DATE(COALESCE(node_updated_at, updated_at)) AS node_day
                    FROM (
                        SELECT human_case_id, node_name, node_status, node_updated_at, updated_at,
                               ROW_NUMBER() OVER (PARTITION BY human_case_id, node_name ORDER BY id DESC) AS rn
                        FROM human_case_node
                        WHERE project_uuid = %s
                          AND node_name IN ({node_placeholders})
                    ) t
                    WHERE rn = 1
                    """,
                    [project_uuid] + node_names,
                )
                node_rows = list(cur.fetchall())
                nodes_by_case = defaultdict(dict)
                for node in node_rows:
                    nodes_by_case[node["human_case_id"]][node["node_name"]] = {
                        "status": int(node.get("node_status") or 0),
                        "day": str(node.get("node_day") or ""),
                    }

                for row in case_rows:
                    row["project_id"] = project_uuid
                    row["project_name"] = project_name
                    row["video_seconds"] = float(row.get("video_seconds") or 0.0)
                    row["delivery_video_seconds"] = float(row.get("delivery_video_seconds") or 0.0)
                    row["nodes"] = nodes_by_case.get(row["id"], {})
                    all_cases.append(row)
    finally:
        conn.close()

    if not all_cases:
        result = _empty_result(project_config)
        result["linked_projects"] = linked_projects
        return result

    qc_rows = []
    qc_pass_rows = []
    sem_rows = []
    pose_rows = []
    lab_rows = []
    lab_done_rows = []
    packed_rows = []
    deliver_inspect_rows = []

    sem_done_rows = []
    pose_done_rows = []

    for case in all_cases:
        nodes = case["nodes"]
        base_row = _base_metric_row(case)
        video_hours = float(case.get("video_seconds") or 0.0) / 3600.0
        packed_hours = (
            float(case.get("delivery_video_seconds") or 0.0)
        ) / 3600.0

        samp_status = _latest_node_status(nodes, "human_case_sampling")
        insp_status = _latest_node_status(nodes, "human_case_inspect")

        pass_cnt = 0
        fail_cnt = 0
        pending_inspect_h = 0.0
        pending_sampling_h = 0.0

        if samp_status == 3:
            pass_cnt = 1
        elif samp_status is None and insp_status == 3:
            pass_cnt = 1
        elif samp_status == 4:
            fail_cnt = 1
        elif samp_status in (1, 2) and insp_status == 3:
            fail_cnt = 1
        elif samp_status is None and insp_status == 4:
            fail_cnt = 1

        if samp_status in (1, 2):
            pending_inspect_h = video_hours
            pending_sampling_h = video_hours
        elif samp_status is None and insp_status in (1, 2):
            pending_inspect_h = video_hours

        if pass_cnt or fail_cnt or pending_inspect_h or pending_sampling_h:
            qc_rows.append(
                {
                    "project_id": case["project_id"],
                    "env_type_name": case.get("env_type_name", ""),
                    "environment_num": case.get("environment_num", ""),
                    "env_num": case.get("env_num", ""),
                    "pass_cnt": pass_cnt,
                    "fail_cnt": fail_cnt,
                    "pass_hours": video_hours if pass_cnt else 0.0,
                    "fail_hours": video_hours if fail_cnt else 0.0,
                    "pending_inspect_hours": pending_inspect_h,
                    "pending_sampling_hours": pending_sampling_h,
                }
            )
        if pass_cnt:
            qc_pass_rows.append(dict(base_row, cnt=1, hours=video_hours))

        sem_status = _latest_node_status(nodes, "semantics_labeling", "semantic_labeling")
        pose_status = _latest_node_status(nodes, "pose_labeling")
        if sem_status == 1:
            sem_rows.append(dict(base_row, cnt=1, hours=video_hours))
        if sem_status == 3:
            sem_done_rows.append(dict(base_row, cnt=1, hours=video_hours))
        if pose_status == 1:
            pose_rows.append(dict(base_row, cnt=1, hours=video_hours))
        if pose_status == 3:
            pose_done_rows.append(dict(base_row, cnt=1, hours=video_hours))
        if sem_status == 1 or pose_status == 1:
            lab_rows.append(dict(base_row, cnt=1, hours=video_hours))
        _lab_done_node = lab_done_node or "labeling_complete"
        if _latest_node_status(nodes, _lab_done_node) == 3:
            lab_done_rows.append(dict(base_row, cnt=1, hours=video_hours))
        _packed_node = packed_node or "complete_job"
        if _latest_node_status(nodes, _packed_node, "delivery_packaging") == 3:
            packed_rows.append(dict(base_row, cnt=1, hours=packed_hours))
        if has_deliver_inspect and _latest_node_status(nodes, "deliver_inspect") == 3:
            deliver_inspect_rows.append(dict(base_row, cnt=1, hours=packed_hours))

    lab_done_daily, collect_done_daily, qc_pass_daily = _build_daily_arrays_from_cases(all_cases, lab_done_node=lab_done_node)

    _agg = lambda rows, sm=scene_mapping: aggregate(rows, sm, allow_all=allow_all_scenes)
    qc_pass_k, qc_pass_u = _agg(qc_pass_rows)
    sem_ing_k, sem_ing_u = _agg(sem_rows)
    sem_done_k, sem_done_u = _agg(sem_done_rows)
    pose_ing_k, pose_ing_u = _agg(pose_rows)
    pose_done_k, pose_done_u = _agg(pose_done_rows)
    lab_ing_k, lab_ing_u = _agg(lab_rows)
    lab_done_k, lab_done_u = _agg(lab_done_rows)
    packed_k, packed_u = _agg(_apply_task_cap_rows(packed_rows, packed_task_cap_hours))
    deliver_inspect_k = None
    deliver_inspect_dedup_k = None
    if has_deliver_inspect:
        deliver_inspect_k, _ = _agg(deliver_inspect_rows)
        if dedup:
            if dedup_mode == "task_producer_global_cap":
                deliver_inspect_dedup_k, _ = _agg(
                    _global_dedup_and_cap(deliver_inspect_rows, dedup_task_cap_hours),
                )
            else:
                deliver_inspect_dedup_k, _ = _agg(
                    _dedup_metric_rows(deliver_inspect_rows, dedup_mode=dedup_mode, task_cap_hours=dedup_task_cap_hours),
                )
    qc_known, qc_unknown = aggregate_qc_compat(qc_rows, scene_mapping, allow_all=allow_all_scenes)

    pending_by_project = defaultdict(float)
    for row in qc_rows:
        pending_by_project[row["project_id"]] += float(row.get("pending_inspect_hours") or 0.0)
    qc_pending_project = [
        {
            "project_id": case["project_id"],
            "project_name": case["project_name"],
            "pending_inspect_h": pending_by_project.get(case["project_id"], 0.0),
        }
        for case in {c["project_id"]: c for c in all_cases}.values()
        if pending_by_project.get(case["project_id"], 0.0) > 0
    ]
    qc_pending_project.sort(key=lambda x: x["pending_inspect_h"], reverse=True)

    qc_pass_dedup_k = qc_pass_dedup_u = None
    sem_ing_dedup_k = sem_ing_dedup_u = None
    pose_ing_dedup_k = pose_ing_dedup_u = None
    lab_ing_dedup_k = lab_ing_dedup_u = None
    lab_done_dedup_k = lab_done_dedup_u = None
    packed_dedup_k = packed_dedup_u = None
    if dedup:
        # task_producer_global_cap 模式：全局去重+封顶
        if dedup_mode == "task_producer_global_cap":
            def _global_dedup_and_cap(rows, cap_hours):
                if not rows:
                    return []
                # 全局 (task_name, producer) 去重，保留最大 case_id
                picked = {}
                for row in rows:
                    producer = (row.get("producer") or "").strip()
                    if not producer:
                        continue
                    key = (row.get("task_name") or "", producer)
                    prev = picked.get(key)
                    if prev is None or int(row.get("case_id") or 0) > int(prev.get("case_id") or 0):
                        picked[key] = dict(row)
                # 应用任务级封顶
                return _apply_task_cap_rows(list(picked.values()), cap_hours)

            qc_pass_dedup_k, qc_pass_dedup_u = aggregate(
                _global_dedup_and_cap(qc_pass_rows, dedup_task_cap_hours),
                scene_mapping,
            )
            sem_ing_dedup_k, sem_ing_dedup_u = aggregate(
                _global_dedup_and_cap(sem_rows, dedup_task_cap_hours),
                scene_mapping,
            )
            pose_ing_dedup_k, pose_ing_dedup_u = aggregate(
                _global_dedup_and_cap(pose_rows, dedup_task_cap_hours),
                scene_mapping,
            )
            lab_ing_dedup_k, lab_ing_dedup_u = aggregate(
                _global_dedup_and_cap(lab_rows, dedup_task_cap_hours),
                scene_mapping,
            )
            lab_done_dedup_k, lab_done_dedup_u = aggregate(
                _global_dedup_and_cap(lab_done_rows, dedup_task_cap_hours),
                scene_mapping,
            )
            if packed_dedup_enabled:
                packed_dedup_rows_newdb = _global_dedup_and_cap(
                    packed_rows,
                    packed_dedup_task_cap_hours if packed_dedup_task_cap_hours is not None else dedup_task_cap_hours
                )
                packed_dedup_k, packed_dedup_u = aggregate(
                    packed_dedup_rows_newdb,
                    scene_mapping,
                )
        else:
            # 其他模式：使用原有逻辑
            qc_pass_dedup_k, qc_pass_dedup_u = aggregate(
                _dedup_metric_rows(qc_pass_rows, dedup_mode=dedup_mode, task_cap_hours=dedup_task_cap_hours),
                scene_mapping,
            )
            sem_ing_dedup_k, sem_ing_dedup_u = aggregate(
                _dedup_metric_rows(sem_rows, dedup_mode=dedup_mode, task_cap_hours=dedup_task_cap_hours),
                scene_mapping,
            )
            pose_ing_dedup_k, pose_ing_dedup_u = aggregate(
                _dedup_metric_rows(pose_rows, dedup_mode=dedup_mode, task_cap_hours=dedup_task_cap_hours),
                scene_mapping,
            )
            lab_ing_dedup_k, lab_ing_dedup_u = aggregate(
                _dedup_metric_rows(lab_rows, dedup_mode=dedup_mode, task_cap_hours=dedup_task_cap_hours),
                scene_mapping,
            )
            lab_done_dedup_k, lab_done_dedup_u = aggregate(
                _dedup_metric_rows(lab_done_rows, dedup_mode=dedup_mode, task_cap_hours=dedup_task_cap_hours),
                scene_mapping,
            )
            if packed_dedup_enabled:
                packed_dedup_rows_newdb = _dedup_metric_rows(
                    packed_rows,
                    dedup_mode=dedup_mode,
                    task_cap_hours=(
                        packed_dedup_task_cap_hours
                        if packed_dedup_task_cap_hours is not None
                        else dedup_task_cap_hours
                    ),
                )
                packed_dedup_k, packed_dedup_u = aggregate(
                    packed_dedup_rows_newdb,
                    scene_mapping,
                )

    daily_actual_dedup = bool(project_config.get("daily_actual_dedup", False))
    daily_cases = _dedup_daily_cases(all_cases, daily_actual_dedup)
    daily_actual = {
        "collect_done_hours": 0.0,
        "qc_pass_hours": 0.0,
        "label_done_hours": 0.0,
    }
    for case in daily_cases:
        nodes = case["nodes"]
        video_hours = float(case.get("video_seconds") or 0.0) / 3600.0
        if (
            _latest_node_status(nodes, "human_case_produce_complete") == 3
            and _latest_node_day(nodes, "human_case_produce_complete") == today_str
        ):
            daily_actual["collect_done_hours"] += video_hours
        if (
            _latest_node_status(nodes, "labeling_complete") == 3
            and _latest_node_day(nodes, "labeling_complete") == today_str
        ):
            daily_actual["label_done_hours"] += video_hours
        if (
            _latest_node_status(nodes, "human_case_sampling") == 3
            and _latest_node_day(nodes, "human_case_sampling") == today_str
        ):
            daily_actual["qc_pass_hours"] += video_hours
        elif (
            _latest_node_status(nodes, "human_case_sampling") is None
            and _latest_node_status(nodes, "human_case_inspect") == 3
            and _latest_node_day(nodes, "human_case_inspect") == today_str
        ):
            daily_actual["qc_pass_hours"] += video_hours

    all_unknown = defaultdict(lambda: {"hours": 0.0, "cnt": 0})
    for uk in (qc_pass_u, sem_ing_u, sem_done_u, pose_ing_u, pose_done_u, lab_ing_u, lab_done_u, packed_u):
        for key, value in uk.items():
            all_unknown[key]["hours"] += value["hours"]
            all_unknown[key]["cnt"] += value["cnt"]
    for key, value in qc_unknown.items():
        all_unknown[key]["cnt"] = max(
            all_unknown[key]["cnt"],
            value["pass"] + value["fail"],
        )

    return {
        "known": {
            "qc_pass": qc_pass_k,
            "sem_ing": sem_ing_k,
            "sem_done": sem_done_k,
            "pose_ing": pose_ing_k,
            "pose_done": pose_done_k,
            "lab_ing": lab_ing_k,
            "lab_done": lab_done_k,
            "packed": packed_k,
            "qc_scene": dict(qc_known),
            "lab_done_daily": lab_done_daily,
            "collect_done_daily": collect_done_daily,
            "qc_pass_daily": qc_pass_daily,
            "pending_by_project": qc_pending_project,
            "qc_pass_dedup": qc_pass_dedup_k,
            "sem_ing_dedup": sem_ing_dedup_k,
            "pose_ing_dedup": pose_ing_dedup_k,
            "lab_ing_dedup": lab_ing_dedup_k,
            "lab_done_dedup": lab_done_dedup_k,
            "packed_dedup": packed_dedup_k,
            "deliver_inspect": deliver_inspect_k,
            "deliver_inspect_dedup": deliver_inspect_dedup_k,
        },
        "unknown": dict(all_unknown),
        "dedup": dedup,
        "daily": {
            "date": today_str,
            "actual": daily_actual,
            "target": daily_target,
        },
        "linked_projects": linked_projects,
        "_packed_dedup_rows": packed_dedup_rows_newdb if (dedup and packed_dedup_enabled) else [],
    }


def run_project(project_config, scene_mapping):
    query_tag = project_config.get("query_tag")
    if query_tag:
        project_entries = fetch_projects_by_tag_all_sources(query_tag)
    else:
        project_entries = project_config.get("query_projects", [])
    newdb_only = bool(project_config.get("newdb_only", False))
    asset_projects = [] if newdb_only else [p for p in project_entries if p.get("source", "asset") != "newdb"]
    newdb_projects = [p for p in project_entries if p.get("source") == "newdb"]

    results = []
    if asset_projects:
        asset_result = run_project_asset(project_config, scene_mapping, asset_projects)
        asset_result["linked_projects"] = _project_link_rows(asset_projects)
        results.append(asset_result)
    if newdb_projects:
        results.append(run_project_newdb(project_config, scene_mapping, newdb_projects))

    if not results:
        return _empty_result(project_config)

    dedup_mode = project_config.get("dedup_mode", "task_producer")
    needs_global_cap = (
        dedup_mode in ("cap_only", "task_producer_global_cap")
        and bool(project_config.get("packed_dedup_enabled", True))
    )

    if len(results) == 1 and not needs_global_cap:
        return results[0]

    if len(results) == 1:
        merged = results[0]
    else:
        merged = _empty_result(project_config)
        metric_keys = [
            "qc_pass",
            "sem_ing",
            "sem_done",
            "pose_ing",
            "pose_done",
            "lab_ing",
            "lab_done",
            "packed",
            "qc_pass_dedup",
            "sem_ing_dedup",
            "pose_ing_dedup",
            "lab_ing_dedup",
            "lab_done_dedup",
            "packed_dedup",
            "deliver_inspect",
            "deliver_inspect_dedup",
        ]
        for key in metric_keys:
            maps = [item["known"].get(key) for item in results if item["known"].get(key) is not None]
            merged["known"][key] = None if not maps else _merge_hours_maps(maps)

        merged["known"]["qc_scene"] = _merge_qc_scene_maps(
            [item["known"].get("qc_scene") for item in results]
        )
        merged["known"]["pending_by_project"] = _merge_pending_projects(
            [item["known"].get("pending_by_project") for item in results]
        )
        for arr_key in ("lab_done_daily", "collect_done_daily", "qc_pass_daily"):
            merged["known"][arr_key] = _merge_daily_arrays(
                [item["known"].get(arr_key) for item in results]
            )
        merged["unknown"] = _merge_unknown_maps([item.get("unknown") for item in results])
        merged["dedup"] = any(item.get("dedup") for item in results)
        merged["daily"] = _merge_daily(results)
        merged["linked_projects"] = [
            row
            for item in results
            for row in item.get("linked_projects", [])
        ]

    # 跨库全局处理（cap_only / task_producer_global_cap 模式 + packed_dedup）
    if needs_global_cap:
        packed_dedup_task_cap_hours = _to_float_or_none(
            project_config.get("packed_dedup_task_cap_hours")
            or project_config.get("dedup_task_cap_hours")
        )
        all_packed_rows = []
        for item in results:
            all_packed_rows.extend(item.get("_packed_dedup_rows", []))
        if all_packed_rows and packed_dedup_task_cap_hours:
            if dedup_mode == "task_producer_global_cap":
                # 先全局按 (task_name, producer) 去重，再封顶
                dedup_picked = {}
                for row in all_packed_rows:
                    producer = (row.get("producer") or "").strip()
                    if not producer:
                        continue
                    key = (row.get("task_name") or "", producer)
                    prev = dedup_picked.get(key)
                    if prev is None or int(row.get("case_id") or 0) > int(prev.get("case_id") or 0):
                        dedup_picked[key] = row
                all_packed_rows = list(dedup_picked.values())
            globally_capped = _apply_task_cap_rows(all_packed_rows, packed_dedup_task_cap_hours)
            packed_dedup_k, _ = aggregate(globally_capped, scene_mapping)
            merged["known"]["packed_dedup"] = packed_dedup_k

    return merged


# ── 报告格式化 ────────────────────────────────────────────────────────────────

def build_scene_order(project_config, known_metrics):
    # 有配置目标时按配置顺序；无目标时展示本次查询中出现的全部已识别环境
    configured = [s["name"] for s in project_config.get("scenes", [])]
    if configured:
        return configured

    scenes = set()
    for metric_key in ("qc_pass", "sem_ing", "pose_ing", "lab_ing", "lab_done", "packed"):
        metric_map = known_metrics.get(metric_key)
        if isinstance(metric_map, dict):
            scenes.update(metric_map.keys())
    qc_scene = known_metrics.get("qc_scene", {})
    if isinstance(qc_scene, dict):
        scenes.update(qc_scene.keys())
    return sorted(scenes)

def format_report(project_config, data):
    name    = project_config["name"]
    dname   = project_config.get("display_name", name)
    ddate   = project_config.get("delivery_date", "—")
    total_h = project_config.get("base_total_hours") or project_config.get("target_total_hours")
    dedup   = data.get("dedup", False)
    show_deliver_inspect = bool(project_config.get("show_deliver_inspect", False))

    known   = data["known"]
    unknown = data["unknown"]
    daily   = data.get("daily", {})

    scenes_conf = [s["name"] for s in project_config.get("scenes", [])]
    scene_cfg = {s["name"]: s for s in project_config.get("scenes", [])}
    order = build_scene_order(project_config, known)

    # 识别关联采集项目中出现但未在交付项目中配置的环境（已映射的场景名）
    all_scenes = set()
    hours_by_scene = {}
    for metric_key in ("qc_pass", "sem_ing", "pose_ing", "lab_ing", "lab_done", "packed"):
        m = known.get(metric_key)
        if isinstance(m, dict):
            for scene, v in m.items():
                all_scenes.add(scene)
                hours_by_scene[scene] = hours_by_scene.get(scene, 0.0) + float(v.get("hours") or 0.0)
    extra_scenes = sorted(s for s in all_scenes if s not in scenes_conf)

    def get_h(metric, scene):
        m = known.get(metric)
        if not m:
            return 0
        return m.get(scene, {}).get("hours", 0)

    def total(metric, apply_scene_cap=False):
        m = known.get(metric)
        if not m:
            return 0
        total_hours = 0.0
        for scene, v in m.items():
            hours = float(v.get("hours") or 0.0)
            if apply_scene_cap and metric in ("packed", "packed_dedup"):
                tgt_h = _to_float_or_none(scene_cfg.get(scene, {}).get("target_hours"))
                if tgt_h is not None:
                    hours = min(hours, tgt_h)
            total_hours += hours
        return total_hours

    total_packed_raw = total("packed")
    total_packed = total("packed", apply_scene_cap=True)
    progress_pct = (total_packed / total_h * 100) if total_h and total_h > 0 else None
    if progress_pct is not None:
        progress_pct = min(progress_pct, 100.0)
    status = ("✅" if progress_pct and progress_pct >= 100
              else "⚠️" if progress_pct and progress_pct >= 70
              else "🔴")

    lines = []
    lines.append(f"## {dname} {status}  交付：{ddate}" +
                 (f" | 目标：{total_h}h" if total_h else ""))
    lines.append("")

    linked_projects = data.get("linked_projects") or []
    if linked_projects:
        asset_names = [p["name"] for p in linked_projects if p.get("source", "asset") != "newdb"]
        newdb_names = [p["name"] for p in linked_projects if p.get("source") == "newdb"]
        source_parts = []
        if asset_names:
            source_parts.append(f"主库 {len(asset_names)} 个")
        if newdb_names:
            source_parts.append(f"新库 {len(newdb_names)} 个")
        if source_parts:
            lines.append("关联采集项目：" + " / ".join(source_parts))
        if newdb_names:
            lines.append("新库采集项目：" + " / ".join(newdb_names))
        lines.append("")

    # ── 一、交付进度统计 ──
    lines.append("### 一、交付进度统计")
    lines.append("")
    if scenes_conf:
        lines.append("本项目按配置统计以下环境：" + " / ".join(scenes_conf))
        if extra_scenes:
            extra_desc = []
            for s in extra_scenes:
                h_val = hours_by_scene.get(s, 0.0)
                extra_desc.append(f"{s}{h(h_val)}")
            lines.append("（关联采集项目中还存在未配置环境：" + "，".join(extra_desc) + "，未纳入统计）")
    else:
        lines.append("本项目未配置环境目标，以下统计包含已识别全部环境。")
    lines.append("")

    if dedup:
        di_header = " 交付质检 | 交付质检(去重) |" if show_deliver_inspect else ""
        di_sep    = "----------|-----------------|" if show_deliver_inspect else ""
        lines.append(f"| 环境 | 采集质检成功 | 采集质检成功(去重) | 语义标注中 | 语义标注中(去重) | 手势标注中 | 手势标注中(去重) | 标注中 | 标注中(去重) | 标注完成 | 标注完成(去重) | 打包成功 | 打包成功(去重) |{di_header} 目标 | 进度 |")
        lines.append(f"|------|------------|-----------------|-----------|----------------|-----------|----------------|--------|------------|---------|--------------|---------|--------------|{di_sep}------|------|")
    else:
        di_header = " 交付质检 |" if show_deliver_inspect else ""
        di_sep    = "----------|" if show_deliver_inspect else ""
        lines.append(f"| 环境 | 采集质检成功 | 语义标注中 | 手势标注中 | 标注中 | 标注完成 | 打包成功 |{di_header} 目标 | 进度 |")
        lines.append(f"|------|---------|-----------|-----------|--------|---------|---------|{di_sep}------|------|")

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

        if tgt_h:
            tgt_str  = f"{tgt_h}h"
            prog_val = min(pkg / tgt_h * 100, 100.0) if tgt_h > 0 else 0.0
            prog_str = f"{prog_val:.1f}%"
        elif ratio_min and total_h:
            lo = total_h * ratio_min
            hi = total_h * (ratio_max or ratio_min)
            tgt_str = f"{lo:.0f}~{hi:.0f}h"
            curr = pkg / total_packed_raw * 100 if total_packed_raw > 0 else 0
            prog_str = f"占{curr:.1f}% (目标{ratio_min*100:.0f}~{(ratio_max or ratio_min)*100:.0f}%)"
        else:
            tgt_str  = "—"
            prog_str = "—"

        if dedup:
            qc_dedup   = get_h("qc_pass_dedup",  scene)
            sem_dedup  = get_h("sem_ing_dedup",  scene)
            pos_dedup  = get_h("pose_ing_dedup", scene)
            lab_dedup  = get_h("lab_ing_dedup",  scene)
            ldn_dedup  = get_h("lab_done_dedup", scene)
            pkg_dedup  = get_h("packed_dedup",   scene)
            di_cell    = f" {h(get_h('deliver_inspect', scene))} |" if show_deliver_inspect else ""
            lines.append(
                f"| {scene} | {h(qc)} | {h(qc_dedup)} | {h(sem)} | {h(sem_dedup)} | {h(pos)} | {h(pos_dedup)} | {h(lab)} | {h(lab_dedup)} | {h(ldn)} | {h(ldn_dedup)} | {h(pkg)} | {h(pkg_dedup)} |{di_cell} {tgt_str} | {prog_str} |"
            )
        else:
            di_cell = f" {h(get_h('deliver_inspect', scene))} |" if show_deliver_inspect else ""
            lines.append(
                f"| {scene} | {h(qc)} | {h(sem)} | {h(pos)} | {h(lab)} | {h(ldn)} | {h(pkg)} |{di_cell} {tgt_str} | {prog_str} |"
            )

    if dedup:
        total_qc_dedup   = total("qc_pass_dedup")
        total_sem_dedup  = total("sem_ing_dedup")
        total_pos_dedup  = total("pose_ing_dedup")
        total_lab_dedup  = total("lab_ing_dedup")
        total_ldn_dedup  = total("lab_done_dedup")
        total_pkg_dedup  = total("packed_dedup", apply_scene_cap=True)
        if show_deliver_inspect:
            di_total_cell = f" **{h(total('deliver_inspect'))}** | **{h(total('deliver_inspect_dedup'))}** |"
        else:
            di_total_cell = ""
        lines.append(
            f"| **总计** | **{h(total('qc_pass'))}** | **{h(total_qc_dedup)}** |"
            f" **{h(total('sem_ing'))}** | **{h(total_sem_dedup)}** |"
            f" **{h(total('pose_ing'))}** | **{h(total_pos_dedup)}** |"
            f" **{h(total('lab_ing'))}** | **{h(total_lab_dedup)}** |"
            f" **{h(total('lab_done'))}** | **{h(total_ldn_dedup)}** |"
            f" **{h(total_packed)}** | **{h(total_pkg_dedup)}** |"
            f"{di_total_cell}"
            f" **{f'{total_h}h' if total_h else '—'}** |"
            f" **{f'{progress_pct:.1f}%' if progress_pct is not None else '—'}** |"
        )
    else:
        di_total_cell = f" **{h(total('deliver_inspect'))}** |" if show_deliver_inspect else ""
        lines.append(
            f"| **总计** | **{h(total('qc_pass'))}** | **{h(total('sem_ing'))}** |"
            f" **{h(total('pose_ing'))}** | **{h(total('lab_ing'))}** |"
            f" **{h(total('lab_done'))}** | **{h(total_packed)}** |"
            f"{di_total_cell}"
            f" **{f'{total_h}h' if total_h else '—'}** |"
            f" **{f'{progress_pct:.1f}%' if progress_pct is not None else '—'}** |"
        )
    lines.append("")

    # ── 二、质检状态 ──
    lines.append("### 二、质检状态")
    lines.append("")
    lines.append("| 环境 | 待质检时长 | 待抽检时长 | 质检通过 | 质检失败 | 通过率 |")
    lines.append("|------|-----------|-----------|---------|---------|--------|")

    t_pend_insp = t_pend_samp = t_pass = t_fail = 0.0
    for scene in order:
        s    = known["qc_scene"].get(scene, {})
        pend_insp = s.get("pending_inspect_h", 0)
        pend_samp = s.get("pending_sampling_h", 0)
        pas  = s.get("pass", 0)
        fai  = s.get("fail", 0)
        if pas + fai + pend_insp + pend_samp == 0:
            continue
        t_pend_insp += pend_insp
        t_pend_samp += pend_samp
        t_pass += pas
        t_fail += fai
        rate = f"{pas / (pas + fai) * 100:.1f}%" if (pas + fai) > 0 else "—"
        lines.append(f"| {scene} | {h(pend_insp)} | {h(pend_samp)} | {pas:,} | {fai:,} | {rate} |")

    t_rate = (f"{t_pass / (t_pass + t_fail) * 100:.1f}%"
              if (t_pass + t_fail) > 0 else "—")
    lines.append(
        f"| **总计** | **{h(t_pend_insp)}** | **{h(t_pend_samp)}** | **{int(t_pass):,}** | **{int(t_fail):,}** | **{t_rate}** |"
    )
    lines.append("")

    # 待质检时长按采集项目分布（只展示项目名）
    pending_proj = sorted(known.get("pending_by_project", []), key=lambda x: x.get("pending_inspect_h", 0), reverse=True)
    lines.append("#### 待质检时长（采集项目分布）")
    lines.append("")
    lines.append("| 采集项目 | 待质检时长 |")
    lines.append("|----------|-----------|")
    nonzero_rows = 0
    pending_total_h = 0.0
    for item in pending_proj:
        hours = float(item.get("pending_inspect_h") or 0.0)
        if hours <= 0:
            continue
        pending_total_h += hours
        nonzero_rows += 1
        lines.append(f"| {item.get('project_name', item.get('project_id'))} | {h(hours)} |")
    if nonzero_rows == 0:
        lines.append("| （无） | 0.0h |")
    lines.append(f"| **总计** | **{h(pending_total_h)}** |")
    lines.append("")

    # ── 三、今日目标达成 ──
    day_str = daily.get("date", datetime.now().strftime("%Y-%m-%d"))
    daily_actual = daily.get("actual", {})
    daily_target = daily.get("target", {})
    lines.append(f"### 三、今日目标达成（{day_str}）")
    lines.append("")
    lines.append("| 指标 | 今日实际 | 今日目标 | 达成率 |")
    lines.append("|------|---------|---------|--------|")

    any_target = False
    for metric_key, (_, _, metric_label) in DAILY_NODE_RULES.items():
        actual_h = float(daily_actual.get(metric_key) or 0.0)
        target_h = _to_float_or_none(daily_target.get(metric_key))
        if target_h is not None:
            any_target = True
        target_str = h(target_h) if target_h is not None else "—"
        if target_h and target_h > 0:
            rate = f"{actual_h / target_h * 100:.1f}%"
        else:
            rate = "—"
        lines.append(f"| {metric_label} | {h(actual_h)} | {target_str} | {rate} |")

    if not any_target:
        lines.append("")
        lines.append("> 未配置今日目标，可通过 `python3 manage.py set-daily-goals` 预先设置。")
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
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--project", help="项目名")
    group.add_argument("--all", action="store_true", help="查询所有活跃项目")
    group.add_argument("--tag", help="按项目标签查询（自动拉取标签关联项目）")
    parser.add_argument("--no-save", action="store_true", help="不保存快照")
    parser.add_argument("--json", action="store_true", help="输出 JSON 到 stdout（供 server.py 调用）")
    args = parser.parse_args()

    with open(CONFIG_PATH, encoding="utf-8") as f:
        config = json.load(f)

    scene_mapping = config.get("scene_mapping", {})
    active = [p for p in config.get("projects", []) if p.get("status") == "active"]

    if not active and not args.tag:
        print("❌ 没有活跃项目，请先通过 manage.py add 添加项目。")
        sys.exit(1)

    if args.tag:
        tagged_projects = fetch_projects_by_tag_all_sources(args.tag)
        if not tagged_projects:
            print(f"❌ 未找到标签 '{args.tag}' 关联的有效项目。")
            sys.exit(1)
        targets_list = [{
            "name": f"tag_{args.tag}",
            "display_name": f"标签 {args.tag}",
            "delivery_date": "—",
            "base_total_hours": None,
            "scenes": [],
            "query_projects": [
                {
                    "id": r["id"],
                    "name": r.get("name", r["id"]),
                    "source": r.get("source", "asset"),
                }
                for r in tagged_projects
            ],
            "status": "active",
        }]
    elif args.project:
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
        print("\n用法：--project <名称> / --all / --tag <标签名>")
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

    query_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if args.json:
        # server.py 调用时输出 JSON，其余全部到 stderr
        output = {
            "query_time": query_time,
            "projects": {
                name: {
                    "config":  r["config"],
                    "known":   r["data"]["known"],
                    "unknown": r["data"]["unknown"],
                    "daily":   r["data"].get("daily", {}),
                }
                for name, r in results.items()
            },
        }
        print(json.dumps(output, ensure_ascii=False))
        return

    # stdout：Markdown 报告
    print(f"> 查询时间：{query_time}\n")
    for name, r in results.items():
        print(format_report(r["config"], r["data"]))
        print()

    # 完整数据写文件
    os.makedirs(REPORT_DIR, exist_ok=True)
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    if args.tag:
        safe_tag = re.sub(r"[^0-9A-Za-z_-]+", "_", args.tag.strip()) or "tag"
        tag = f"tag_{safe_tag}"
    else:
        tag = args.project if args.project else "all"
    out_path = os.path.join(REPORT_DIR, f"delivery_{tag}_{ts}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({
            "query_time": query_time,
            "projects": {
                name: {
                    "config":   r["config"],
                    "known":    r["data"]["known"],
                    "unknown":  r["data"]["unknown"],
                    "daily":    r["data"].get("daily", {}),
                }
                for name, r in results.items()
            },
        }, f, ensure_ascii=False, indent=2)
    print(f"\n详细数据 → {out_path}")


if __name__ == "__main__":
    main()
