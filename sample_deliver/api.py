import time
import os

import requests
import certifi
from loguru import logger
from typing import List, Optional


class AssetAPI:
    def __init__(self, username: str, token: str, env: str = "prod", max_speed_mbps: Optional[float] = None):
        if env == "dev":
            self.base_url = "https://assetserver-dev.lightwheel.net"
        elif env == "prod":
            self.base_url = "https://assetserver.lightwheel.net"
        else:
            raise ValueError(f"Invalid environment: {env}")

        self.username = username
        self.token = token
        self.max_speed_mbps = max_speed_mbps

    @property
    def _headers(self) -> dict:
        token = self.token if self.token.startswith("Bearer") else f"Bearer {self.token}"
        return {
            "Authorization": token,
            "Username": self.username,
            "Content-Type": "application/json",
        }

    def get_completed_cases(self, project_id: str, limit: int = 0) -> List[dict]:
        """获取项目中打包完成的 cases（nodeName=complete_job, nodeStatus=3）"""
        url = f"{self.base_url}/api/asset/v2/human-case/list"
        params = {
            "projectUuid": project_id,
            "equal": {
                "nodeName": "complete_job",
                "nodeStatus": 3,
            },
            "page": 1,
            "pageSize": 10000,
        }
        response = requests.post(url, headers=self._headers, json=params)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch cases: {response.text}")

        cases = response.json().get("data", [])
        if not cases:
            raise Exception("No completed cases found for this project")

        if limit and limit > 0:
            cases = cases[:limit]

        logger.info(f"Fetched {len(cases)} cases (limit={limit or 'none'})")
        return cases

    def get_case_files(self, human_case_id: str, file_names: List[str] = []) -> List[dict]:
        """获取指定 case 的文件下载链接"""
        url = f"{self.base_url}/api/asset/v1/human-case/get-files"
        headers = {k: v for k, v in self._headers.items() if k != "Content-Type"}
        headers["username"] = self.username
        params = {
            "humanCaseId": human_case_id,
            "fileNames": file_names,
            "return_private_url": True,
        }
        res = requests.post(url=url, headers=headers, json=params)
        if res.status_code != 200:
            raise Exception(f"Failed to get files for case {human_case_id}: {res.text}")
        return res.json().get("files", [])

    def download_file(self, url: str, output_path: str, progress_callback=None):
        """下载单个文件，支持限速和进度回调"""
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        if os.path.exists(output_path):
            logger.info(f"Already exists, skip: {output_path}")
            return

        chunk_size = 64 * 1024
        time_per_chunk = (chunk_size / (self.max_speed_mbps * 1024 * 1024)) if self.max_speed_mbps else None

        response = requests.get(url, stream=True, verify=certifi.where())
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))
        downloaded = 0

        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    t0 = time.perf_counter()
                    f.write(chunk)
                    downloaded += len(chunk)

                    if progress_callback:
                        progress_callback(len(chunk), total_size)

                    if time_per_chunk:
                        elapsed = time.perf_counter() - t0
                        if elapsed < time_per_chunk:
                            time.sleep(time_per_chunk - elapsed)
