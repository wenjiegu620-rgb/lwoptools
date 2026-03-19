"""
sample_deliver.tool
-------------------
Agent-facing entry point. Compatible with OpenClaw, LangChain, AutoGen, and
any framework that wraps plain Python functions as tools.

Usage (direct):
    from sample_deliver import download_project_data
    result = download_project_data(
        project_id="b96edd5c-...",
        username="wenjie.gu",
        token="eyJ...",
        limit=10,
        output_dir="./downloads",
    )

Usage (OpenClaw / LangChain tool decorator):
    from langchain.tools import tool
    download_tool = tool(download_project_data)
"""

import os
from typing import Optional

from loguru import logger

from .api import AssetAPI
from .downloader import process_case
from .report import generate_report


def download_project_data(
    project_id: str,
    username: str,
    token: str,
    limit: int = 0,
    output_dir: str = "./downloads",
    env: str = "prod",
    max_speed_mbps: float = 0.0,
) -> dict:
    """
    Download completed (packed) cases from the Lightwheel asset platform for a
    given project, and generate an Excel delivery report.

    Args:
        project_id:      Project UUID on the Lightwheel platform.
        username:        Platform account name (e.g. "wenjie.gu").
        token:           Bearer token for authentication.
        limit:           Max number of cases to download. 0 means download all.
        output_dir:      Local directory to save files and the report.
        env:             "prod" (default) or "dev".
        max_speed_mbps:  Download speed cap in MB/s. 0 means unlimited.

    Returns:
        {
            "success": bool,
            "total_available": int,   # total completed cases on platform
            "downloaded": int,        # cases actually downloaded
            "num_tasks": int,
            "tasks": [str, ...],      # unique task names
            "report_path": str,       # absolute path to Excel report
            "output_dir": str,
            "error": str | None,
        }
    """
    os.makedirs(output_dir, exist_ok=True)

    result_base = {
        "success": False,
        "total_available": 0,
        "downloaded": 0,
        "num_tasks": 0,
        "tasks": [],
        "report_path": None,
        "output_dir": os.path.abspath(output_dir),
        "error": None,
    }

    try:
        api = AssetAPI(
            username=username,
            token=token,
            env=env,
            max_speed_mbps=max_speed_mbps if max_speed_mbps > 0 else None,
        )

        # 获取所有打包完成的 cases（limit 在 API 层截断）
        logger.info(f"Fetching completed cases for project {project_id} ...")
        cases = api.get_completed_cases(project_id=project_id, limit=limit)
        total_available = len(cases)  # 已经过 limit 截断，反映实际拉取数
        result_base["total_available"] = total_available
        logger.info(f"Found {total_available} cases to download")

        # 逐条下载
        results = []
        for idx, case in enumerate(cases, 1):
            logger.info(f"[{idx}/{total_available}] Processing case {case.get('id')}")
            r = process_case(case, output_dir, api)
            if r:
                results.append(r)

        # 生成交付报告
        report_path = generate_report(
            project_id=project_id,
            total_available=total_available,
            results=results,
            output_dir=output_dir,
        )

        unique_tasks = sorted(set(r["task_name"] for r in results))
        logger.success(
            f"Done. Downloaded {len(results)}/{total_available} cases, "
            f"{len(unique_tasks)} tasks. Report: {report_path}"
        )

        return {
            **result_base,
            "success": True,
            "downloaded": len(results),
            "num_tasks": len(unique_tasks),
            "tasks": unique_tasks,
            "report_path": report_path,
        }

    except Exception as e:
        logger.error(f"download_project_data failed: {e}")
        return {**result_base, "error": str(e)}
