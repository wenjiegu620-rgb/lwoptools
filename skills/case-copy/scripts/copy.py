#!/usr/bin/env python3
"""
case_copy/copy.py
通过 Asset API 将指定 case 批量复制到目标项目。

用法：
  python3 copy.py \
    --token "Bearer xxx" --username yyy \
    --src 774f145e-... \
    --dst e172f294-... \
    --ids "uuid1,uuid2,uuid3" \
    [--env prod]
"""

import argparse
import json
import sys

import requests


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--token", required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument("--src", required=True, help="源项目 UUID")
    parser.add_argument("--dst", required=True, help="目标项目 UUID")
    parser.add_argument("--ids", required=True, help="逗号分隔的 case UUID 列表")
    parser.add_argument("--env", default="prod", choices=["prod", "dev"])
    args = parser.parse_args()

    base = {
        "prod": "https://assetserver.lightwheel.net",
        "dev":  "https://assetserver-dev.lightwheel.net",
    }[args.env]

    bearer = args.token if args.token.startswith("Bearer") else f"Bearer {args.token}"
    headers = {
        "Authorization": bearer,
        "Username": args.username,
        "Content-Type": "application/json",
    }

    ids = [i.strip() for i in args.ids.split(",") if i.strip()]
    if not ids:
        print("ERROR: --ids 不能为空", file=sys.stderr)
        sys.exit(1)

    body = {
        "current_project_uuid": args.src,
        "target_project_uuid":  args.dst,
        "human_case_ids":       ids,
    }

    resp = requests.post(
        f"{base}/api/asset/v2/human-case/copy-human-case",
        headers=headers,
        json=body,
        timeout=30,
    )
    resp.raise_for_status()
    print(json.dumps(resp.json(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
