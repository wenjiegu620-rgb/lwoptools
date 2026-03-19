import os
import shutil
import zipfile
import json
import traceback
from glob import glob
from typing import Optional, Tuple, List

from loguru import logger

from .api import AssetAPI


def _extract_zip(zip_path: str, output_folder: str):
    os.makedirs(output_folder, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as f:
        f.extractall(output_folder)


def _read_json(file_path: str) -> dict:
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def process_case(case: dict, download_dir: str, api: AssetAPI) -> Optional[dict]:
    """
    下载并处理单个 case。

    Returns:
        {
            "task_name": str,
            "episode_uuid": str,
            "files": [str, ...]   # 相对于 download_dir 的路径列表
        }
        或 None（失败时）
    """
    human_case_id = case.get("id")
    task_name = case.get("taskName", "unknown_task")

    try:
        # Step 1: 获取 episode_uuid
        metadata_files = api.get_case_files(human_case_id, file_names=["metadata_trim.json"])
        if len(metadata_files) != 1:
            logger.warning(f"No metadata_trim.json for case {human_case_id}, skip")
            return None

        metadata_path = os.path.join(download_dir, f"_tmp_meta_{human_case_id}.json")
        api.download_file(metadata_files[0]["fileUrl"], metadata_path)
        metadata = _read_json(metadata_path)
        episode_uuid = metadata["task_info"]["episode_uuid"]
        os.remove(metadata_path)

        # Step 2: 获取需要下载的文件列表
        files_to_download = api.get_case_files(
            human_case_id,
            file_names=["task.zip", "episode.mcap", "episode_vis.mcap"]
        )
        if len(files_to_download) < 2:
            logger.warning(f"Missing required files for case {human_case_id}, skip")
            return None

        # Step 3: 从 task.zip 中读取真实 task_name
        for file_info in files_to_download:
            if file_info["fileName"] == "task.zip":
                zip_path = os.path.join(download_dir, f"_tmp_task_{human_case_id}.zip")
                extract_dir = os.path.join(download_dir, f"_tmp_extract_{human_case_id}")
                api.download_file(file_info["fileUrl"], zip_path)
                _extract_zip(zip_path, extract_dir)
                for item in glob(os.path.join(extract_dir, "**", "metadata.json"), recursive=True):
                    inner = _read_json(item)
                    task_name = inner.get("task_info", {}).get("task_name", task_name)
                    break
                os.remove(zip_path)
                shutil.rmtree(extract_dir)
                break

        # Step 4: 创建目录并下载所有文件
        task_dir = os.path.join(download_dir, task_name, episode_uuid)
        os.makedirs(task_dir, exist_ok=True)
        downloaded_files: List[str] = []

        for file_info in files_to_download:
            file_name = file_info["fileName"]
            file_url = file_info["fileUrl"]

            if file_name == "task.zip":
                zip_path = os.path.join(download_dir, f"_tmp_task_{human_case_id}.zip")
                extract_dir = os.path.join(download_dir, f"_tmp_extract_{human_case_id}")
                api.download_file(file_url, zip_path)
                _extract_zip(zip_path, extract_dir)

                for item in glob(os.path.join(extract_dir, "**", "*"), recursive=True):
                    if os.path.isfile(item):
                        parts = os.path.relpath(item, extract_dir).split(os.sep)
                        if len(parts) > 2:
                            rel = os.path.join(*parts[2:])
                            dst = os.path.join(task_dir, rel)
                            os.makedirs(os.path.dirname(dst), exist_ok=True)
                            shutil.move(item, dst)
                            downloaded_files.append(os.path.join(task_name, episode_uuid, rel))

                os.remove(zip_path)
                shutil.rmtree(extract_dir)

            elif file_name == "episode.mcap":
                out = os.path.join(download_dir, task_name, f"{episode_uuid}.mcap")
                api.download_file(file_url, out)
                downloaded_files.append(os.path.join(task_name, f"{episode_uuid}.mcap"))

            elif file_name == "episode_vis.mcap":
                out = os.path.join(download_dir, task_name, f"{episode_uuid}_vis.mcap")
                api.download_file(file_url, out)
                downloaded_files.append(os.path.join(task_name, f"{episode_uuid}_vis.mcap"))

        logger.success(f"Done: {task_name} / {episode_uuid}")
        return {
            "task_name": task_name,
            "episode_uuid": episode_uuid,
            "files": downloaded_files,
        }

    except Exception as e:
        logger.error(f"Failed case {human_case_id}: {e}\n{traceback.format_exc()}")
        return None
