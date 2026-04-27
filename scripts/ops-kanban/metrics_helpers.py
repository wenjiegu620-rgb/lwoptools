from collections import defaultdict
from datetime import datetime


def dedupe_case_rows(rows):
      """按 (db_source, case_id) 去重，避免同一 case 被重复累计"""
      merged = {}

      for row in rows or []:
          case_id = str(row.get("case_id") or "")
          db_source = row.get("db_source") or "unknown"

          if case_id:
              key = (db_source, case_id)
          else:
              key = (
                  db_source,
                  row.get("producer"),
                  row.get("vendor") or row.get("producer_group"),
                  row.get("t_start"),
                  row.get("t_end"),
                  float(row.get("vsec") or 0),
              )

          if key not in merged:
              merged[key] = dict(row)
              continue

          cur = merged[key]

          if row.get("t_start") and (not cur.get("t_start") or row["t_start"] < cur["t_start"]):
              cur["t_start"] = row["t_start"]
          if row.get("t_end") and (not cur.get("t_end") or row["t_end"] > cur["t_end"]):
              cur["t_end"] = row["t_end"]

          cur_vsec = float(cur.get("vsec") or 0)
          row_vsec = float(row.get("vsec") or 0)
          if row_vsec > cur_vsec:
              cur["vsec"] = row["vsec"]

          if not cur.get("producer") and row.get("producer"):
              cur["producer"] = row["producer"]
          if not cur.get("vendor") and row.get("vendor"):
              cur["vendor"] = row["vendor"]

          if not cur.get("project_id") and row.get("project_id"):
              cur["project_id"] = row["project_id"]

      result = list(merged.values())
      result.sort(key=lambda r: (
          str(r.get("producer") or ""),
          r.get("t_start") or r.get("t_end") or datetime.min,
      ))
      return result


def aggregate_collect_rows(rows):
      """把去重后的 case rows 聚合成按采集员统计所需的基础结构"""
      sessions_by_p = defaultdict(list)
      vendor_by_p = {}
      vsec_by_p = defaultdict(float)
      cases_by_p = defaultdict(int)
      project_ids_by_p = defaultdict(set)

      for row in rows or []:
          producer = row.get("producer")
          if not producer:
              continue

          vendor_by_p[producer] = row.get("vendor") or row.get("producer_group") or "未知"
          sessions_by_p[producer].append((row.get("t_start"), row.get("t_end")))
          vsec_by_p[producer] += float(row.get("vsec") or 0)
          cases_by_p[producer] += 1

          project_id = row.get("project_id")
          if project_id:
              project_ids_by_p[producer].add(project_id)

      return {
          "sessions_by_p": sessions_by_p,
          "vendor_by_p": vendor_by_p,
          "vsec_by_p": vsec_by_p,
          "cases_by_p": cases_by_p,
          "project_ids_by_p": project_ids_by_p,
      }


def calc_online_hours(sessions_by_p, gap_sec=30 * 60):
      """根据采集打点时间计算在线时长、首次出现、最后出现时间"""
      online_h_map = {}
      first_seen_map = {}
      last_seen_map = {}

      for producer, segs in (sessions_by_p or {}).items():
          cleaned = [(s, e) for s, e in segs if s and e]
          if not cleaned:
              online_h_map[producer] = 0.0
              first_seen_map[producer] = ""
              last_seen_map[producer] = ""
              continue

          points = sorted(set(e for _, e in cleaned))
          if not points:
              online_h_map[producer] = 0.0
              first_seen_map[producer] = ""
              last_seen_map[producer] = ""
              continue

          online_sec = 0
          seg_start = points[0]
          seg_end = points[0]

          for pt in points[1:]:
              if (pt - seg_end).total_seconds() <= gap_sec:
                  seg_end = pt
              else:
                  online_sec += (seg_end - seg_start).total_seconds()
                  seg_start = pt
                  seg_end = pt

          online_sec += (seg_end - seg_start).total_seconds()

          online_h_map[producer] = round(online_sec / 3600, 2)
          first_seen_map[producer] = cleaned[0][0].strftime("%H:%M")
          last_seen_map[producer] = cleaned[-1][1].strftime("%H:%M")

      return {
          "online_h_map": online_h_map,
          "first_seen_map": first_seen_map,
          "last_seen_map": last_seen_map,
      }