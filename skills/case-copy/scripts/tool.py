#!/usr/bin/env python3
"""
case_copy: 从源项目按质检节点状态筛选 human case，批量复制到目标项目，并输出 Excel 报告。

直接运行，按提示输入即可：
  python tool.py
"""

import getpass
import sys
from datetime import datetime

import pandas as pd
import requests
from loguru import logger


NODE_NAME = "human_case_inspect"
STATUS_SUCCESS = 3
STATUS_FAIL = 4

ENV_BASE_URLS = {
    "prod": "https://assetserver.lightwheel.net",
    "dev": "https://assetserver-dev.lightwheel.net",
}

COPY_SUCCESS_LABEL = "复制成功"
COPY_PENDING_LABEL = "已提交复制，接口未返回逐条结果"
COPY_FAILED_LABEL = "复制失败/跳过"


def normalize_env(env: str) -> str:
    normalized = (env or "").strip().lower()
    if normalized not in ENV_BASE_URLS:
        raise ValueError(f"环境必须是 prod 或 dev，收到：{env}")
    return normalized


def extract_case_id(case: dict) -> str:
    return str(case.get("id") or case.get("uuid") or "").strip()


def parse_case_list(payload: dict) -> list[dict]:
    data = payload.get("data") or []
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        if isinstance(data.get("list"), list):
            return data["list"]
        if isinstance(data.get("records"), list):
            return data["records"]
        return [item for item in data.values() if isinstance(item, dict)]
    return []


def _extract_id_set(container) -> set[str]:
    if isinstance(container, dict):
        values = container.values()
    elif isinstance(container, list):
        values = container
    else:
        return set()

    ids: set[str] = set()
    for item in values:
        if isinstance(item, dict):
            candidate = extract_case_id(item)
            if not candidate:
                candidate = str(
                    item.get("human_case_id")
                    or item.get("humanCaseId")
                    or item.get("case_id")
                    or item.get("caseId")
                    or ""
                ).strip()
        else:
            candidate = str(item).strip()
        if candidate:
            ids.add(candidate)
    return ids


def summarize_copy_result(result: dict, requested_ids: list[str]) -> dict[str, set[str]]:
    requested = {case_id for case_id in requested_ids if case_id}
    data = result.get("data")

    confirmed_keys = (
        "successIds",
        "success_ids",
        "copiedIds",
        "copied_ids",
        "humanCaseIds",
        "human_case_ids",
        "ids",
        "list",
        "records",
    )
    failed_keys = (
        "failedIds",
        "failed_ids",
        "duplicateIds",
        "duplicate_ids",
        "duplicatedIds",
        "duplicated_ids",
        "skippedIds",
        "skipped_ids",
        "existsIds",
        "exists_ids",
    )

    confirmed_ids: set[str] = set()
    failed_ids: set[str] = set()

    if isinstance(data, dict):
        for key in confirmed_keys:
            confirmed_ids |= _extract_id_set(data.get(key))
        for key in failed_keys:
            failed_ids |= _extract_id_set(data.get(key))
    elif isinstance(data, list):
        confirmed_ids |= _extract_id_set(data)

    confirmed_ids &= requested
    failed_ids &= requested

    unknown_ids = requested - confirmed_ids - failed_ids

    top_level_ok = result.get("success")
    if top_level_ok is None:
        code = result.get("code")
        top_level_ok = code in (0, 200, "0", "200")

    if not confirmed_ids and not failed_ids and top_level_ok:
        unknown_ids = set(requested)

    return {
        "confirmed_ids": confirmed_ids,
        "failed_ids": failed_ids,
        "unknown_ids": unknown_ids,
    }


class AssetAPI:
    def __init__(self, token: str, username: str, env: str = "prod"):
        self.base = ENV_BASE_URLS[normalize_env(env)]
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
        cases = parse_case_list(self._post("/api/asset/v2/human-case/list", body))
        logger.info(f"  获取到 {len(cases)} 条（status={node_status}）")
        return cases[:count]

    def copy_cases(
        self,
        src_project_uuid: str,
        dst_project_uuid: str,
        case_ids: list[str],
    ) -> dict:
        body = {
            "current_project_uuid": src_project_uuid,
            "target_project_uuid": dst_project_uuid,
            "human_case_ids": case_ids,
        }
        return self._post("/api/asset/v2/human-case/copy-human-case", body)


def build_report_rows(
    success_cases: list[dict],
    fail_cases: list[dict],
    copy_summary: dict[str, set[str]],
) -> list[dict]:
    copied_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    confirmed_ids = copy_summary["confirmed_ids"]
    failed_ids = copy_summary["failed_ids"]
    unknown_ids = copy_summary["unknown_ids"]

    rows = []
    for cases, qc_status in (
        (success_cases, "质检通过"),
        (fail_cases, "质检不通过"),
    ):
        for case in cases:
            case_id = extract_case_id(case)
            if case_id in confirmed_ids:
                copy_status = COPY_SUCCESS_LABEL
            elif case_id in failed_ids:
                copy_status = COPY_FAILED_LABEL
            elif case_id in unknown_ids:
                copy_status = COPY_PENDING_LABEL
            else:
                copy_status = "未返回该 case 的复制结果"

            rows.append(
                {
                    "case_name": case.get("name", ""),
                    "case_uuid": case_id,
                    "batch_name": case.get("batchName", ""),
                    "task_name": case.get("taskName", ""),
                    "description": case.get("description", ""),
                    "qc_status": qc_status,
                    "copy_status": copy_status,
                    "copied_at": copied_at,
                }
            )
    return rows


def build_report(
    success_cases: list[dict],
    fail_cases: list[dict],
    copy_summary: dict[str, set[str]],
    output: str,
):
    rows = build_report_rows(success_cases, fail_cases, copy_summary)
    df = pd.DataFrame(
        rows,
        columns=[
            "case_name",
            "case_uuid",
            "batch_name",
            "task_name",
            "description",
            "qc_status",
            "copy_status",
            "copied_at",
        ],
    )

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="复制记录")
        ws = writer.sheets["复制记录"]
        for col in ws.columns:
            max_len = max(len(str(cell.value or "")) for cell in col)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 60)

    logger.success(f"Excel 已生成：{output}（共 {len(rows)} 条）")


def prompt(msg: str, default: str = "") -> str:
    hint = f"（默认：{default}）" if default else ""
    val = input(f"{msg}{hint}：").strip()
    return val or default


def main():
    print("=" * 50)
    print("  Human Case 批量复制工具")
    print("=" * 50)

    username = prompt("用户名")
    token = getpass.getpass("Bearer token（输入不回显）：").strip()
    src_project = prompt("源项目 UUID")
    dst_project = prompt("目标项目 UUID")
    count_str = prompt("每种状态复制条数", "20")
    output = prompt("输出 Excel 文件名", "case_copy_report.xlsx")
    env = prompt("环境 prod/dev", "prod")

    if not username or not token or not src_project or not dst_project:
        logger.error("用户名、token、源/目标项目 UUID 均不能为空")
        sys.exit(1)

    try:
        count = int(count_str)
    except ValueError:
        logger.error(f"条数必须是整数，收到：{count_str}")
        sys.exit(1)

    try:
        env = normalize_env(env)
    except ValueError as exc:
        logger.error(str(exc))
        sys.exit(1)

    print()
    api = AssetAPI(token, username, env)

    logger.info(f"[1/4] 获取质检通过 cases（最多 {count} 条）...")
    success_cases = api.list_cases(src_project, STATUS_SUCCESS, count)

    logger.info(f"[2/4] 获取质检不通过 cases（最多 {count} 条）...")
    fail_cases = api.list_cases(src_project, STATUS_FAIL, count)

    all_ids = [extract_case_id(case) for case in success_cases + fail_cases]
    all_ids = [case_id for case_id in all_ids if case_id]
    logger.info(f"[3/4] 共 {len(all_ids)} 条，复制到目标项目 {dst_project}...")

    if not all_ids:
        logger.warning("没有可复制的 case，退出。")
        sys.exit(0)

    result = api.copy_cases(src_project, dst_project, all_ids)
    copy_summary = summarize_copy_result(result, all_ids)
    confirmed_count = len(copy_summary["confirmed_ids"])
    failed_count = len(copy_summary["failed_ids"])
    unknown_count = len(copy_summary["unknown_ids"])

    logger.success(
        "复制请求完成：已确认成功 {} 条，失败/跳过 {} 条，待人工确认 {} 条".format(
            confirmed_count, failed_count, unknown_count
        )
    )
    if unknown_count:
        logger.warning("接口未返回逐条复制结果，Excel 中已标记“已提交复制，接口未返回逐条结果”。")

    logger.info("[4/4] 生成 Excel 报告...")
    build_report(success_cases, fail_cases, copy_summary, output)


if __name__ == "__main__":
    main()
