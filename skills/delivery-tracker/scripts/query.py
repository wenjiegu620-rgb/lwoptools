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
    "host":            os.environ.get(
        "DELIVERY_DB_HOST",
        "rm-uf69cxp907m8j6k4a.mysql.rds.aliyuncs.com",
    ),
    "port":            int(os.environ.get("DELIVERY_DB_PORT", "3306")),
    "user":            os.environ.get("DELIVERY_DB_USER", "wenjie.gu"),
    "password":        os.environ.get("DELIVERY_DB_PASSWORD", "Lightwheel*2026"),
    "database":        os.environ.get("DELIVERY_DB_NAME", "asset"),
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

HOURS_EXPR = "SUM(IFNULL(hc.video_seconds, 0)) / 3600.0"
PACKED_HOURS_EXPR = "SUM(COALESCE(hc.delivery_video_seconds, hc.video_seconds, 0)) / 3600.0"
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
    """打包成功：complete_job 取最新记录 status=3，时长取 delivery_video_seconds"""
    ph = _ph(project_ids)
    cur.execute(f"""
        SELECT hc.project_id, {ENV_FIELDS},
               COUNT(*) AS cnt, {PACKED_HOURS_EXPR} AS hours
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


# ── 去重查询（task_name + producer 各取 1 条）───────────────────────────────

def query_node_dedup(cur, project_ids, node_name, node_status):
    """节点统计（去重版）：同 task_name + producer 只取最新 1 条"""
    ph = _ph(project_ids)
    cur.execute(f"""
        SELECT hc.project_id, {ENV_FIELDS},
               COUNT(*) AS cnt, {HOURS_EXPR} AS hours
        FROM human_cases hc
        WHERE hc.project_id IN ({ph})
          AND hc.producer != ''
          AND hc.id IN (
              SELECT MAX(hc2.id)
              FROM human_cases hc2
              WHERE hc2.project_id IN ({ph})
                AND hc2.producer != ''
                AND EXISTS (
                    SELECT 1 FROM human_case_nodes hcn2
                    WHERE hcn2.human_case_id = hc2.id
                      AND hcn2.node_name = %s AND hcn2.node_status = %s
                )
              GROUP BY hc2.task_name,
                       hc2.producer
          )
        GROUP BY hc.project_id, env_type_name, environment_num, env_num
    """, project_ids + project_ids + [node_name, node_status])
    return list(cur.fetchall())


def query_qc_pass_dedup_compat(cur, project_ids):
    """
    采集质检成功（去重版，兼容 sampling）：
      - dedup 口径：同 task_name + producer 只取最新 1 条
      - 质检口径：
          有 sampling 节点 -> sampling=3 才算通过
          无 sampling 节点 -> inspect=3 才算通过
    """
    ph = _ph(project_ids)
    cur.execute(f"""
        SELECT hc.project_id, {ENV_FIELDS},
               COUNT(*) AS cnt, {HOURS_EXPR} AS hours
        FROM human_cases hc
        WHERE hc.project_id IN ({ph})
          AND hc.producer != ''
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
                      WHERE project_id IN ({ph}) AND node_name = 'sampling'
                      GROUP BY human_case_id
                  ) latest_samp ON latest_samp.max_id = hcn.id
              ) samp ON samp.human_case_id = hc2.id
              WHERE hc2.project_id IN ({ph})
                AND hc2.producer != ''
                AND (
                    samp.node_status = 3
                    OR (samp.node_status IS NULL AND insp.node_status = 3)
                )
              GROUP BY hc2.task_name, hc2.producer
          )
        GROUP BY hc.project_id, env_type_name, environment_num, env_num
    """, project_ids + project_ids + project_ids + project_ids)
    return list(cur.fetchall())


def query_labeling_inprogress_dedup(cur, project_ids):
    """标注中（去重版）：semantics OR pose，同 task_name + producer 只取最新 1 条"""
    ph = _ph(project_ids)
    cur.execute(f"""
        SELECT hc.project_id, {ENV_FIELDS},
               COUNT(*) AS cnt, {HOURS_EXPR} AS hours
        FROM human_cases hc
        WHERE hc.project_id IN ({ph})
          AND hc.producer != ''
          AND hc.id IN (
              SELECT MAX(hc2.id)
              FROM human_cases hc2
              WHERE hc2.project_id IN ({ph})
                AND hc2.producer != ''
                AND EXISTS (
                    SELECT 1 FROM human_case_nodes hcn2
                    WHERE hcn2.human_case_id = hc2.id
                      AND hcn2.node_name IN ('semantics_labeling', 'pose_labeling')
                      AND hcn2.node_status = 1
                )
              GROUP BY hc2.task_name,
                       hc2.producer
          )
        GROUP BY hc.project_id, env_type_name, environment_num, env_num
    """, project_ids + project_ids)
    return list(cur.fetchall())


def query_packaged_dedup(cur, project_ids):
    """打包成功（去重版）：同 task_name + producer 只取最新 1 条，时长取 delivery_video_seconds"""
    ph = _ph(project_ids)
    cur.execute(f"""
        SELECT hc.project_id, {ENV_FIELDS},
               COUNT(*) AS cnt, {PACKED_HOURS_EXPR} AS hours
        FROM human_cases hc
        WHERE hc.project_id IN ({ph})
          AND hc.producer != ''
          AND hc.id IN (
              SELECT MAX(hc2.id)
              FROM human_cases hc2
              INNER JOIN (
                  SELECT hcn.human_case_id
                  FROM human_case_nodes hcn
                  INNER JOIN (
                      SELECT human_case_id, MAX(id) AS max_id
                      FROM human_case_nodes
                      WHERE project_id IN ({ph}) AND node_name = 'complete_job'
                      GROUP BY human_case_id
                  ) latest ON hcn.id = latest.max_id
                  WHERE hcn.node_status = 3
              ) packed ON hc2.id = packed.human_case_id
              WHERE hc2.project_id IN ({ph})
                AND hc2.producer != ''
              GROUP BY hc2.task_name,
                       hc2.producer
          )
        GROUP BY hc.project_id, env_type_name, environment_num, env_num
    """, project_ids + project_ids + project_ids)
    return list(cur.fetchall())


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
                WHERE project_id IN ({ph}) AND node_name = 'sampling'
                GROUP BY human_case_id
            ) latest_samp ON latest_samp.max_id = hcn.id
        ) samp ON samp.human_case_id = hc.id
        WHERE hc.project_id IN ({ph})
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
                      AND node_name = 'sampling'
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
                        AND s.node_name = 'sampling'
                        AND s.human_case_id = hcn.human_case_id
                  )
            ) q ON q.human_case_id = hc2.id
            WHERE hc2.project_id IN ({ph})
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
                  AND node_name = 'sampling'
                  AND DATE(COALESCE(node_updated_at, updated_at)) = %s
                GROUP BY human_case_id
            ) latest_samp ON latest_samp.max_id = hcn.id
            WHERE hcn.node_status = 3
        ) s ON s.human_case_id = hc.id
        WHERE hc.project_id IN ({ph})
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
                    AND s.node_name = 'sampling'
                    AND s.human_case_id = hcn.human_case_id
              )
        ) i ON i.human_case_id = hc.id
        WHERE hc.project_id IN ({ph})
    """, project_ids + [day_str] + project_ids + project_ids)
    rows = list(cur.fetchall())
    inspect_pass_without_sampling_h = float((rows[0] if rows else {}).get("hours") or 0.0)

    return {
        "collect_done_hours": collect_done_h,
        "qc_pass_hours": sampling_pass_h + inspect_pass_without_sampling_h,
        "label_done_hours": label_done_h,
    }


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


def aggregate_qc_compat(rows, scene_mapping):
    known = defaultdict(lambda: {
        "pass": 0,
        "fail": 0,
        "pending_h": 0.0,
        "pending_inspect_h": 0.0,
        "pending_sampling_h": 0.0,
    })
    unknown = defaultdict(lambda: {
        "pass": 0,
        "fail": 0,
        "pending_h": 0.0,
        "pending_inspect_h": 0.0,
        "pending_sampling_h": 0.0,
    })

    for row in rows:
        env_key = parse_env_key(row)
        scene = resolve_scene(env_key, scene_mapping)
        target = known[scene] if scene is not None else unknown[env_key or "(空)"]

        pass_cnt = int(row.get("pass_cnt") or 0)
        fail_cnt = int(row.get("fail_cnt") or 0)
        pending_inspect_h = float(row.get("pending_inspect_hours") or 0.0)
        pending_sampling_h = float(row.get("pending_sampling_hours") or 0.0)

        target["pass"] += pass_cnt
        target["fail"] += fail_cnt
        target["pending_inspect_h"] += pending_inspect_h
        target["pending_sampling_h"] += pending_sampling_h
        target["pending_h"] += pending_inspect_h

    return dict(known), dict(unknown)


def h(val):
    return f"{val:.1f}h" if val else "0.0h"


def log(msg):
    print(f"  {msg}", file=sys.stderr)


# ── 主查询 ────────────────────────────────────────────────────────────────────

def run_project(project_config, scene_mapping):
    project_ids = [p["id"] for p in project_config["query_projects"]]
    dedup = project_config.get("dedup_by_producer_scene", False)

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
            today_str = datetime.now().strftime("%Y-%m-%d")
            log(f"今日目标达成（{today_str}）...")
            daily_actual_dedup = (project_config.get("name") == "mango_500h")
            daily_actual = query_daily_actuals(
                cur,
                project_ids,
                today_str,
                dedup_by_task_producer=daily_actual_dedup,
            )
            daily_target = resolve_daily_goals(project_config, today_str)

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
                    query_qc_pass_dedup_compat(cur, project_ids), scene_mapping)
                log("语义标注中（去重）...")
                sem_ing_dedup_k, sem_ing_dedup_u = aggregate(
                    query_node_dedup(cur, project_ids, "semantics_labeling", 1), scene_mapping)
                log("手势标注中（去重）...")
                pose_ing_dedup_k, pose_ing_dedup_u = aggregate(
                    query_node_dedup(cur, project_ids, "pose_labeling", 1), scene_mapping)
                log("标注中（去重，去重版）...")
                lab_ing_dedup_k, lab_ing_dedup_u = aggregate(
                    query_labeling_inprogress_dedup(cur, project_ids), scene_mapping)
                log("标注完成（去重）...")
                lab_done_dedup_k, lab_done_dedup_u = aggregate(
                    query_node_dedup(cur, project_ids, "labeling_complete", 3), scene_mapping)
                log("打包成功（去重）...")
                packed_dedup_k, packed_dedup_u = aggregate(
                    query_packaged_dedup(cur, project_ids), scene_mapping)
    finally:
        conn.close()

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
            "qc_pass":        qc_pass_k,
            "sem_ing":        sem_ing_k,
            "pose_ing":       pose_ing_k,
            "lab_ing":        lab_ing_k,
            "lab_done":       lab_done_k,
            "packed":         packed_k,
            "qc_scene":       dict(qc_known),
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
    }


# ── 报告格式化 ────────────────────────────────────────────────────────────────

def build_scene_order(project_config, known_metrics):
    # 只展示项目配置中声明的环境；未配置的环境不出现在报告里
    return [s["name"] for s in project_config.get("scenes", [])]

def format_report(project_config, data):
    name    = project_config["name"]
    dname   = project_config.get("display_name", name)
    ddate   = project_config.get("delivery_date", "—")
    total_h = project_config.get("base_total_hours") or project_config.get("target_total_hours")
    dedup   = data.get("dedup", False)

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

    # ── 一、交付进度统计 ──
    lines.append("### 一、交付进度统计")
    lines.append("")
    lines.append("本项目按配置统计以下环境：" + " / ".join(scenes_conf))
    if extra_scenes:
        extra_desc = []
        for s in extra_scenes:
            h_val = hours_by_scene.get(s, 0.0)
            extra_desc.append(f"{s}{h(h_val)}")
        lines.append("（关联采集项目中还存在未配置环境：" + "，".join(extra_desc) + "，未纳入统计）")
    lines.append("")

    if dedup:
        lines.append("| 环境 | 采集质检成功 | 采集质检成功(去重) | 语义标注中 | 语义标注中(去重) | 手势标注中 | 手势标注中(去重) | 标注中 | 标注中(去重) | 标注完成 | 标注完成(去重) | 打包成功 | 打包成功(去重) | 目标 | 进度 |")
        lines.append("|------|------------|-----------------|-----------|----------------|-----------|----------------|--------|------------|---------|--------------|---------|--------------|------|------|")
    else:
        lines.append("| 环境 | 采集质检成功 | 语义标注中 | 手势标注中 | 标注中 | 标注完成 | 打包成功 | 目标 | 进度 |")
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
            lines.append(
                f"| {scene} | {h(qc)} | {h(qc_dedup)} | {h(sem)} | {h(sem_dedup)} | {h(pos)} | {h(pos_dedup)} | {h(lab)} | {h(lab_dedup)} | {h(ldn)} | {h(ldn_dedup)} | {h(pkg)} | {h(pkg_dedup)} | {tgt_str} | {prog_str} |"
            )
        else:
            lines.append(
                f"| {scene} | {h(qc)} | {h(sem)} | {h(pos)} | {h(lab)} | {h(ldn)} | {h(pkg)} | {tgt_str} | {prog_str} |"
            )

    if dedup:
        total_qc_dedup   = total("qc_pass_dedup")
        total_sem_dedup  = total("sem_ing_dedup")
        total_pos_dedup  = total("pose_ing_dedup")
        total_lab_dedup  = total("lab_ing_dedup")
        total_ldn_dedup  = total("lab_done_dedup")
        total_pkg_dedup  = total("packed_dedup", apply_scene_cap=True)
        lines.append(
            f"| **总计** | **{h(total('qc_pass'))}** | **{h(total_qc_dedup)}** |"
            f" **{h(total('sem_ing'))}** | **{h(total_sem_dedup)}** |"
            f" **{h(total('pose_ing'))}** | **{h(total_pos_dedup)}** |"
            f" **{h(total('lab_ing'))}** | **{h(total_lab_dedup)}** |"
            f" **{h(total('lab_done'))}** | **{h(total_ldn_dedup)}** |"
            f" **{h(total_packed)}** | **{h(total_pkg_dedup)}** |"
            f" **{f'{total_h}h' if total_h else '—'}** |"
            f" **{f'{progress_pct:.1f}%' if progress_pct is not None else '—'}** |"
        )
    else:
        lines.append(
            f"| **总计** | **{h(total('qc_pass'))}** | **{h(total('sem_ing'))}** |"
            f" **{h(total('pose_ing'))}** | **{h(total('lab_ing'))}** |"
            f" **{h(total('lab_done'))}** | **{h(total_packed)}** |"
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
                    "daily":    r["data"].get("daily", {}),
                }
                for name, r in results.items()
            },
        }, f, ensure_ascii=False, indent=2)
    print(f"\n详细数据 → {out_path}")


if __name__ == "__main__":
    main()
