#!/usr/bin/env python3
"""
将 Markdown 报告渲染为 PNG 图片。

用法：
  echo "# 报告" | python3 render.py
  python3 render.py --input report.md
  python3 render.py --input report.md --output /tmp/report.png

输出：图片路径（stdout）
"""

import argparse
import os
import subprocess
import sys
import tempfile
from datetime import datetime

from markdown_it import MarkdownIt
from PIL import Image


HTML_TEMPLATE = """\
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  html, body {{
    font-family: -apple-system, "PingFang SC", "Microsoft YaHei", sans-serif;
    font-size: 15px;
    line-height: 1.65;
    color: #1a1a1a;
    background: #ffffff;
  }}
  body {{
    padding: 28px 32px 32px;
    width: {width}px;
  }}
  h1 {{
    font-size: 20px; font-weight: 700; margin-bottom: 14px; color: #111;
    border-bottom: 2px solid #4a90d9; padding-bottom: 8px;
  }}
  h2 {{
    font-size: 16px; font-weight: 600; margin: 20px 0 10px; color: #222;
    border-bottom: 1px solid #e0e0e0; padding-bottom: 5px;
  }}
  h3 {{ font-size: 15px; font-weight: 600; margin: 14px 0 8px; color: #333; }}
  p {{ margin: 7px 0; }}
  table {{
    border-collapse: collapse;
    margin: 12px 0;
    font-size: 14px;
    table-layout: auto;
    width: auto;           /* 表格宽度尽量贴合内容 */
    max-width: 100%;       /* 同时不超过页面宽度 */
  }}
  th {{
    background: #eef2f7;
    color: #2c3e50;
    font-weight: 600;
    padding: 9px 14px;
    border: 1px solid #d0d7de;
    text-align: left;      /* 表头左对齐 */
    white-space: nowrap;
  }}
  td {{
    padding: 8px 14px;
    border: 1px solid #e1e6eb;
    vertical-align: middle;
    /* 允许内容换行，避免表格比内容大太多 */
    white-space: normal;
  }}
  td:first-child {{ font-weight: 500; }}
  tr:nth-child(even) td {{ background: #f8fafb; }}
  blockquote {{
    border-left: 4px solid #4a90d9;
    margin: 10px 0;
    padding: 8px 14px;
    background: #f0f6ff;
    color: #444;
    border-radius: 0 6px 6px 0;
    font-size: 14px;
  }}
  blockquote p {{ margin: 4px 0; }}
  code {{
    background: #f3f4f6;
    padding: 2px 6px;
    border-radius: 3px;
    font-family: "SF Mono", Menlo, monospace;
    font-size: 13px;
  }}
  ul, ol {{ padding-left: 22px; margin: 7px 0; }}
  li {{ margin: 4px 0; }}
  hr {{ border: none; border-top: 1px solid #e0e0e0; margin: 16px 0; }}
  strong {{ font-weight: 600; }}
  .footer {{
    margin-top: 18px;
    font-size: 12px;
    color: #aaa;
    text-align: right;
  }}
</style>
</head>
<body>
{body}
<div class="footer">生成于 {ts}</div>
</body>
</html>
"""

CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"


def md_to_html(md_text: str, width: int) -> str:
    md = MarkdownIt("commonmark").enable("table")
    body = md.render(md_text)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    return HTML_TEMPLATE.format(body=body, ts=ts, width=width)


def crop_whitespace(img_path: str) -> str:
    """裁掉底部空白，保留内容区域 + 适当下边距。"""
    img = Image.open(img_path).convert("RGB")
    width, height = img.size

    # 从底部向上扫描，找到最后一行非白像素
    last_content_y = 0
    white = (255, 255, 255)
    for y in range(height - 1, -1, -1):
        row = [img.getpixel((x, y)) for x in range(0, width, 4)]
        if any(p != white for p in row):
            last_content_y = y
            break

    crop_height = min(last_content_y + 40, height)  # 40px 下边距
    if crop_height < height:
        img = img.crop((0, 0, width, crop_height))
        img.save(img_path, "PNG", optimize=True)

    return img_path


def render(md_text: str, output_path: str, width: int = 900):
    html = md_to_html(md_text, width)

    with tempfile.NamedTemporaryFile(
        suffix=".html", mode="w", encoding="utf-8", delete=False
    ) as f:
        f.write(html)
        html_path = f.name

    try:
        cmd = [
            CHROME,
            "--headless=new",
            "--no-sandbox",
            "--disable-gpu",
            "--hide-scrollbars",
            f"--screenshot={output_path}",
            f"--window-size={width},5000",
            "--virtual-time-budget=2000",
            html_path,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            print(f"[render error] {result.stderr}", file=sys.stderr)
            sys.exit(1)
    finally:
        os.unlink(html_path)

    if not os.path.exists(output_path):
        print("[render error] 图片未生成", file=sys.stderr)
        sys.exit(1)

    crop_whitespace(output_path)
    return output_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", "-i", help="输入 Markdown 文件（默认 stdin）")
    parser.add_argument(
        "--output", "-o", help="输出 PNG 路径（默认 /tmp/delivery_report_<ts>.png）"
    )
    parser.add_argument("--width", type=int, default=900, help="图片宽度（px），默认 900")
    args = parser.parse_args()

    if args.input:
        with open(args.input, encoding="utf-8") as f:
            md_text = f.read()
    else:
        md_text = sys.stdin.read()

    if not md_text.strip():
        print("[render error] 输入为空", file=sys.stderr)
        sys.exit(1)

    if args.output:
        output_path = args.output
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"/tmp/delivery_report_{ts}.png"

    render(md_text, output_path, args.width)
    print(output_path)


if __name__ == "__main__":
    main()
