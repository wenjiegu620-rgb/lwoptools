#!/usr/bin/env python3
"""
case_copy: 从源项目按质检节点状态筛选 human case，批量复制到目标项目，并输出 Excel 报告。

直接运行，按提示输入即可：
  python tool.py
"""

import os
import sys
import getpass
from datetime import datetime

import requests
import pandas as pd
from loguru import logger


# ── 常量 ────────────────────────────────────────────────────────────────────

NODE_NAME = "human_case_inspect"   # 质检节点
STATUS_SUCCESS = 3                 # 质检成功
STATUS_FAIL    = 4                 # 质检不通过


# ── API 客户端 ───────────────────────────────────────────────────────────────

class AssetAPI:
    def __init__(self, token: str, username: str, env: str = "prod"):
        base = {
            "prod": "https://assetserver.lightwheel.net",
            "dev":  "https://assetserver-dev.lightwheel.net",
        }[env]
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

    def list_cases(self, project_uuid: str, node_status: int, count: int) -> list[dict]:
        """获取指定节点状态的 case 列表（最多 count 条）。"""
        body = {
            "page": 1,
            "pageSize": count,
            "equal": {
                "nodeName": NODE_NAME,
                "nodeStatus": node_status,
            },
            "order": [{"updatedAt": 2}],
            "count": True,
            "projectUuid": project_uuid,
        }
        data = self._post("/api/asset/v2/human-case/list", body)
        cases = data.get("data") or []
        if isinstance(cases, dict):
            cases = list(cases.values())
        logger.info(f"  获取到 {len(cases)} 条（status={node_status}）")
        return cases[:count]

    def get_project_uuid_by_name(self, name: str) -> str | None:
        """通过项目名称模糊查找，返回 UUID。"""
        body = {"page": 1, "pageSize": 50, "name": name}
        data = self._post("/api/asset/v1/project/list", body)
        projects = data.get("data", {}).get("list") or data.get("data") or []
        for p in projects:
            if p.get("name") == name or name in p.get("name", ""):
                return p.get("uuid") or p.get("id")
        return None

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

def build_report(success_cases: list[dict], fail_cases: list[dict], output: str):
    rows = []
    for c in success_cases:
        rows.append({
            "case_name":    c.get("name", ""),
            "case_uuid":    c.get("id", ""),
            "batch_name":   c.get("batchName", ""),
            "task_name":    c.get("taskName", ""),
            "description":  c.get("description", ""),
            "qc_status":    "质检通过",
            "copied_at":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
    for c in fail_cases:
        rows.append({
            "case_name":    c.get("name", ""),
            "case_uuid":    c.get("id", ""),
            "batch_name":   c.get("batchName", ""),
            "task_name":    c.get("taskName", ""),
            "description":  c.get("description", ""),
            "qc_status":    "质检不通过",
            "copied_at":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })

    df = pd.DataFrame(rows, columns=[
        "case_name", "case_uuid", "batch_name", "task_name",
        "description", "qc_status", "copied_at",
    ])

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="复制记录")
        ws = writer.sheets["复制记录"]
        # 简单列宽
        for col in ws.columns:
            max_len = max(len(str(c.value or "")) for c in col)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 60)

    logger.success(f"Excel 已生成：{output}（共 {len(rows)} 条）")


# ── 交互式输入 ───────────────────────────────────────────────────────────────

def prompt(msg: str, default: str = "") -> str:
    hint = f"（默认：{default}）" if default else ""
    val = input(f"{msg}{hint}：").strip()
    return val or default


def main():
    print("=" * 50)
    print("  Human Case 批量复制工具")
    print("=" * 50)

    username    = prompt("用户名")
    token       = getpass.getpass("Bearer token（输入不回显）：").strip()
    src_project = prompt("源项目 UUID")
    dst_project = prompt("目标项目 UUID")
    count_str   = prompt("每种状态复制条数", "20")
    output      = prompt("输出 Excel 文件名", "case_copy_report.xlsx")
    env         = prompt("环境 prod/dev", "prod")

    if not username or not token or not src_project or not dst_project:
        logger.error("用户名、token、源/目标项目 UUID 均不能为空")
        sys.exit(1)

    try:
        count = int(count_str)
    except ValueError:
        logger.error(f"条数必须是整数，收到：{count_str}")
        sys.exit(1)

    print()
    api = AssetAPI(token, username, env)

    # 1. 获取质检通过的 case
    logger.info(f"[1/4] 获取质检通过 cases（最多 {count} 条）...")
    success_cases = api.list_cases(src_project, STATUS_SUCCESS, count)

    # 2. 获取质检不通过的 case
    logger.info(f"[2/4] 获取质检不通过 cases（最多 {count} 条）...")
    fail_cases = api.list_cases(src_project, STATUS_FAIL, count)

    all_ids = [c["id"] for c in success_cases + fail_cases]
    logger.info(f"[3/4] 共 {len(all_ids)} 条，复制到目标项目 {dst_project}...")

    if not all_ids:
        logger.warning("没有可复制的 case，退出。")
        sys.exit(0)

    # 3. 批量复制
    result = api.copy_cases(src_project, dst_project, all_ids)
    logger.success(f"复制完成：{result}")

    # 4. 生成 Excel
    logger.info("[4/4] 生成 Excel 报告...")
    build_report(success_cases, fail_cases, output)


if __name__ == "__main__":
    main()
