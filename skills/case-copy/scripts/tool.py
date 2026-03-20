#!/usr/bin/env python3
"""
case_copy: 从源项目按自定义条件筛选 human case，批量复制到目标项目，并输出 Excel 报告。

直接运行，按提示输入即可：
  python tool.py
"""

import sys
import getpass
from datetime import datetime

import requests
import pandas as pd
from loguru import logger


# ── 节点状态标签（常见值，未知状态显示原始数字）──────────────────────────────

STATUS_LABELS: dict[int, str] = {
    1: "待处理",
    2: "处理中",
    3: "质检通过",
    4: "质检不通过",
    5: "已完成",
}


# ── API 客户端 ───────────────────────────────────────────────────────────────

class AssetAPI:
    def __init__(self, token: str, username: str, env: str = "prod"):
        base = {
            "prod": "https://assetserver.lightwheel.net",
            "dev":  "https://assetserver-dev.lightwheel.net",
        }.get(env)
        if not base:
            logger.error(f"未知环境：{env}，请输入 prod 或 dev")
            sys.exit(1)
        self.base = base
        bearer = token if token.startswith("Bearer") else f"Bearer {token}"
        self.headers = {
            "Authorization": bearer,
            "Username": username,
            "Content-Type": "application/json",
        }

    def _post(self, path: str, body: dict) -> dict:
        url = f"{self.base}{path}"
        resp = requests.post(url, headers=self.headers, json=body, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def get_project_uuid_by_name(self, name: str) -> str | None:
        """通过项目名称模糊查找，返回 UUID。"""
        body = {"page": 1, "pageSize": 50, "name": name}
        data = self._post("/api/asset/v1/project/list", body)
        projects = data.get("data", {}).get("list") or data.get("data") or []
        for p in projects:
            if p.get("name") == name or name in p.get("name", ""):
                return p.get("uuid") or p.get("id")
        return None

    def resolve_project(self, uuid_or_name: str) -> str:
        """
        如果看起来是 UUID（含 -）直接返回；否则按名称查找。
        """
        if "-" in uuid_or_name and len(uuid_or_name) > 30:
            return uuid_or_name
        logger.info(f"  按名称查找项目：{uuid_or_name}")
        uuid = self.get_project_uuid_by_name(uuid_or_name)
        if not uuid:
            logger.error(f"找不到项目：{uuid_or_name}")
            sys.exit(1)
        logger.info(f"  找到 UUID：{uuid}")
        return uuid

    def list_cases(
        self,
        project_uuid: str,
        node_name: str,
        node_status: int,
        count: int,
        task_name: str = "",
    ) -> list[dict]:
        """获取指定节点、状态、task 的 case 列表（最多 count 条）。"""
        equal: dict = {
            "nodeName": node_name,
            "nodeStatus": node_status,
        }
        if task_name:
            equal["taskName"] = task_name

        body = {
            "page": 1,
            "pageSize": count,
            "equal": equal,
            "order": [{"updatedAt": 2}],
            "count": True,
            "projectUuid": project_uuid,
        }
        data = self._post("/api/asset/v2/human-case/list", body)
        cases = data.get("data") or []
        if isinstance(cases, dict):
            cases = list(cases.values())
        label = STATUS_LABELS.get(node_status, f"状态{node_status}")
        logger.info(f"  {label}：获取到 {len(cases)} 条")
        return cases[:count]

    def copy_cases(
        self,
        src_project_uuid: str,
        dst_project_uuid: str,
        case_ids: list[str],
    ) -> dict:
        """批量复制 case 到目标项目。"""
        body = {
            "current_project_uuid": src_project_uuid,
            "target_project_uuid":  dst_project_uuid,
            "human_case_ids":       case_ids,
        }
        return self._post("/api/asset/v2/human-case/copy-human-case", body)


# ── Excel 报告 ───────────────────────────────────────────────────────────────

def build_report(cases_by_status: dict[int, list[dict]], node_name: str, output: str):
    rows = []
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for status, cases in cases_by_status.items():
        label = STATUS_LABELS.get(status, f"状态{status}")
        for c in cases:
            rows.append({
                "case_name":    c.get("name", ""),
                "case_uuid":    c.get("id", ""),
                "batch_name":   c.get("batchName", ""),
                "task_name":    c.get("taskName", ""),
                "description":  c.get("description", ""),
                "node_name":    node_name,
                "node_status":  label,
                "copied_at":    ts,
            })

    df = pd.DataFrame(rows, columns=[
        "case_name", "case_uuid", "batch_name", "task_name",
        "description", "node_name", "node_status", "copied_at",
    ])

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="复制记录")
        ws = writer.sheets["复制记录"]
        for col in ws.columns:
            max_len = max(len(str(c.value or "")) for c in col)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 60)

    logger.success(f"Excel 已生成：{output}（共 {len(rows)} 条）")


# ── 交互式输入 ───────────────────────────────────────────────────────────────

def prompt(msg: str, default: str = "") -> str:
    hint = f"（默认：{default}）" if default else ""
    val = input(f"{msg}{hint}：").strip()
    return val or default


def parse_statuses(raw: str) -> list[int]:
    result = []
    for s in raw.split(","):
        s = s.strip()
        if s:
            try:
                result.append(int(s))
            except ValueError:
                logger.error(f"状态必须是整数，收到：{s}")
                sys.exit(1)
    if not result:
        logger.error("至少指定一个状态码")
        sys.exit(1)
    return result


def main():
    print("=" * 55)
    print("  Human Case 批量复制工具")
    print("=" * 55)
    print()
    print("提示：项目可输入 UUID 或名称关键词")
    print()

    username    = prompt("用户名")
    token       = getpass.getpass("Bearer token（输入不回显）：").strip()
    src_input   = prompt("源项目（UUID 或名称）")
    dst_input   = prompt("目标项目（UUID 或名称）")
    env         = prompt("环境 prod/dev", "prod")
    node_name   = prompt("节点名称", "human_case_inspect")
    statuses_raw = prompt("节点状态（多个用逗号分隔，如 3,4）", "3,4")
    count_str   = prompt("每种状态复制条数", "20")
    task_filter = prompt("Task 名称过滤（留空则不过滤）", "")
    output      = prompt("输出 Excel 文件名", "case_copy_report.xlsx")

    if not username or not token or not src_input or not dst_input:
        logger.error("用户名、token、源/目标项目均不能为空")
        sys.exit(1)

    statuses = parse_statuses(statuses_raw)

    try:
        count = int(count_str)
    except ValueError:
        logger.error(f"条数必须是整数，收到：{count_str}")
        sys.exit(1)

    print()
    api = AssetAPI(token, username, env)

    # 解析项目 UUID
    logger.info("[准备] 解析源项目...")
    src_project = api.resolve_project(src_input)
    logger.info("[准备] 解析目标项目...")
    dst_project = api.resolve_project(dst_input)

    # 按状态逐一获取 case
    cases_by_status: dict[int, list[dict]] = {}
    total_steps = len(statuses) + 2
    for i, status in enumerate(statuses, 1):
        label = STATUS_LABELS.get(status, f"状态{status}")
        task_hint = f"（task={task_filter}）" if task_filter else ""
        logger.info(f"[{i}/{total_steps}] 获取 {label} cases{task_hint}（最多 {count} 条）...")
        cases = api.list_cases(src_project, node_name, status, count, task_filter)
        cases_by_status[status] = cases

    # 汇总去重
    seen: set[str] = set()
    all_ids: list[str] = []
    for cases in cases_by_status.values():
        for c in cases:
            cid = c["id"]
            if cid not in seen:
                seen.add(cid)
                all_ids.append(cid)

    logger.info(f"[{len(statuses)+1}/{total_steps}] 共 {len(all_ids)} 条，复制到目标项目...")

    if not all_ids:
        logger.warning("没有可复制的 case，退出。")
        sys.exit(0)

    result = api.copy_cases(src_project, dst_project, all_ids)
    logger.success(f"复制完成：{result}")

    logger.info(f"[{total_steps}/{total_steps}] 生成 Excel 报告...")
    build_report(cases_by_status, node_name, output)


if __name__ == "__main__":
    main()
