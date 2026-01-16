#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RAZ 绘本批量处理器 - 将 Mineru 导出的 MD 转换为 V4.0 绘本格式
Author: Automation Pipeline
"""

import os
import re
import json
import shutil
import requests
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse
import time

# ==================== 配置 ====================
SOURCE_DIR = Path("/Volumes/disk1/111111/dailyreading/课本存放")
TARGET_DIR = Path("/Volumes/disk1/111111/dailyreading/lesson")
DOWNLOAD_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 2

# 需要移除的无关内容模式
NOISE_PATTERNS = [
    r"^Word Count:\s*\d+.*$",
    r"^A Reading A-Z Level \w+ Leveled Book.*$",
    r"^Level \w+ Leveled Book.*$",
    r"^# Connections\s*$",
    r"^# Writing\s*$",
    r"^# Social Studies\s*$",
    r"^# Focus Question.*$",
    r"^# Words to Know\s*$",
    r"^# Glossary\s*$",
    r"^\$\\odot\$.*$",
    r"^All rights reserved\..*$",
    r"^www\.readinga-z\.com.*$",
    r"^Correlation\s*$",
    r"^<table>.*</table>$",
    r"^Fountas & Pinnell.*$",
    r"^Reading Recovery.*$",
    r"^DRA.*$",
    r"^LEVEL \w+\s*$",
    r"^Retold by.*$",
    r"^Illustrated by.*$",
    r"^\(p\. \d+\).*$",
    r"^echoed \(v\.\).*$",
    r"^pinched \(v\.\).*$",
    r"^shuddered \(v\.\).*$",
    r"^stepmother \(n\.\).*$",
    r"^treasure \(n\.\).*$",
    r"^wicked \(adj\.\).*$",
    r"^repeated a sound.*$",
    r"^held something tightly.*$",
    r"^suddenly shook or trembled.*$",
    r"^a woman who has married.*$",
    r"^something that is very special.*$",
    r"^very mean or bad.*$",
    r"^A German Fairy Tale\s*$",
    r"^How might the story have been different.*$",
    r"^People believe that fairy tales.*$",
    r"^Black Forest.*$",
    r"^it that includes pictures\.\s*$",
    r"^What do you learn about.*$",
    r"^echoed\s*$",
    r"^pinched\s*$",
    r"^shuddered\s*$",
    r"^stepmother\s*$",
    r"^treasure\s*$",
    r"^wicked\s*$",
    r"^Hansel's trail of crumbs\?.*$",
    r"^Hansel and Gretel, are related.*$",
    r"^it that includes pictures\.$",
    r"^Hansel and Gretel\s*$",
]


def print_progress(level: str, book: str, status: str):
    """打印进度信息"""
    print(f"[{level}-Level] Processing: {book}... {status}")


def download_image(url: str, save_path: Path) -> bool:
    """下载图片，带重试机制"""
    for attempt in range(MAX_RETRIES):
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
            }
            response = requests.get(url, timeout=DOWNLOAD_TIMEOUT, headers=headers)
            response.raise_for_status()

            with open(save_path, "wb") as f:
                f.write(response.content)
            return True

        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                print(f"    [Retry {attempt + 1}/{MAX_RETRIES}] Download failed: {e}")
                time.sleep(RETRY_DELAY)
            else:
                print(f"    [Error] Failed to download {url}: {e}")
                return False
    return False


def extract_images(content: str) -> list[str]:
    """提取所有图片 URL"""
    pattern = r"!\[.*?\]\((https?://[^\)]+)\)"
    return re.findall(pattern, content)


def clean_text(content: str) -> str:
    """清洗文本，移除无关信息"""
    lines = content.split("\n")
    cleaned_lines = []
    in_glossary = False
    found_first_image = False
    image_pattern = r"!\[.*?\]\((https?://[^\)]+)\)"

    for line in lines:
        stripped = line.strip()

        # 跳过第一个图片之前的所有内容（封面前的词汇表、介绍等）
        if not found_first_image:
            if re.search(image_pattern, line):
                found_first_image = True
            else:
                continue

        # 跳过 Glossary 部分后的所有内容
        if stripped == "# Glossary":
            in_glossary = True
            continue
        if in_glossary:
            continue

        # 跳过匹配噪音模式的行
        skip = False
        for pattern in NOISE_PATTERNS:
            if re.match(pattern, stripped, re.IGNORECASE):
                skip = True
                break

        # 跳过空的标题行（如 # HANSEL、# GRETEL）
        if re.match(r"^#\s*[A-Z]+\s*$", stripped) and len(stripped.split()) <= 2:
            # 保留真正的标题（如 # Hansel and Gretel）
            if not re.match(r"^#\s+\w+(\s+and\s+\w+)+\s*$", stripped, re.IGNORECASE):
                skip = True

        if not skip:
            cleaned_lines.append(line)

    return "\n".join(cleaned_lines)


def parse_content_blocks(content: str, image_urls: list[str]) -> list[dict]:
    """将内容解析为图文块"""
    blocks = []
    lines = content.split("\n")

    current_text = []
    image_pattern = r"!\[.*?\]\((https?://[^\)]+)\)"

    for line in lines:
        match = re.search(image_pattern, line)
        if match:
            # 保存之前累积的文本
            if current_text:
                text = "\n".join(current_text).strip()
                if text and blocks:
                    blocks[-1]["text"] = text

            # 新增图片块
            url = match.group(1)
            if url in image_urls:
                idx = image_urls.index(url)
                blocks.append({"image_idx": idx + 1, "text": ""})
            current_text = []
        else:
            current_text.append(line)

    # 处理最后的文本
    if current_text:
        text = "\n".join(current_text).strip()
        if text and blocks:
            blocks[-1]["text"] = text

    return blocks


def generate_story_md(blocks: list[dict], book_title: str) -> str:
    """生成 V4.0 格式的 story.md"""
    pages = []

    for i, block in enumerate(blocks):
        img_ref = f"![Page {block['image_idx']}](images/p{block['image_idx']}.jpg)"

        # 第一页添加标题
        if i == 0:
            page_content = f"{img_ref}\n\n# {book_title}"
        else:
            page_content = img_ref

        # 添加正文
        if block["text"]:
            # 清理多余空行
            text = re.sub(r"\n{3,}", "\n\n", block["text"])
            page_content += f"\n\n{text}"

        pages.append(page_content.strip())

    return "\n\n---\n\n".join(pages)


def process_book(source_md: Path, level: str) -> Optional[dict]:
    """处理单本书"""
    book_name = source_md.stem  # 不含扩展名的文件名
    book_title = book_name.title()  # 标题格式化

    print_progress(level, book_name, "Starting...")

    # 创建目标目录
    book_dir = TARGET_DIR / level / book_name
    images_dir = book_dir / "images"
    images_dir.mkdir(parents=True, exist_ok=True)

    # 读取源文件
    with open(source_md, "r", encoding="utf-8") as f:
        content = f.read()

    # 提取图片 URL
    image_urls = extract_images(content)
    print(f"    Found {len(image_urls)} images")

    # 检查是否需要下载图片（增量更新）
    existing_images = list(images_dir.glob("p*.jpg"))
    need_download = len(existing_images) != len(image_urls)

    if need_download:
        # 清空旧图片
        for img in existing_images:
            img.unlink()

        # 下载图片
        for idx, url in enumerate(image_urls, 1):
            save_path = images_dir / f"p{idx}.jpg"
            print(f"    Downloading p{idx}.jpg...")
            if not download_image(url, save_path):
                print(f"    [Warning] Skipped image {idx}")
    else:
        print(
            f"    Images already exist ({len(existing_images)} files), skipping download"
        )

    # 清洗文本
    cleaned_content = clean_text(content)

    # 解析内容块
    blocks = parse_content_blocks(cleaned_content, image_urls)

    # 如果解析结果为空，使用简单模式
    if not blocks:
        print("    [Warning] Block parsing failed, using simple mode")
        blocks = [{"image_idx": i + 1, "text": ""} for i in range(len(image_urls))]

    # 生成 story.md
    story_content = generate_story_md(blocks, book_title)
    story_path = book_dir / "story.md"

    with open(story_path, "w", encoding="utf-8") as f:
        f.write(story_content)

    print_progress(level, book_name, "Done ✓")

    # 返回索引信息
    return {
        "id": book_name,
        "title": book_title,
        "cover": f"lesson/{level}/{book_name}/images/p1.jpg",
        "path": f"lesson/{level}/{book_name}/story.md",
    }


def generate_library_json(library_data: dict):
    """生成 library.json 索引文件"""
    json_path = TARGET_DIR / "library.json"

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(library_data, f, ensure_ascii=False, indent=2)

    print(
        f"\n[Index] Generated library.json with {sum(len(v) for v in library_data.values())} books"
    )


def main():
    """主函数"""
    print("=" * 60)
    print("RAZ 绘本批量处理器 v1.0")
    print("=" * 60)
    print(f"Source: {SOURCE_DIR}")
    print(f"Target: {TARGET_DIR}")
    print("=" * 60 + "\n")

    # 确保目标目录存在
    TARGET_DIR.mkdir(parents=True, exist_ok=True)

    # 扫描所有级别
    library_data = {}
    total_books = 0

    for level_dir in sorted(SOURCE_DIR.iterdir()):
        if not level_dir.is_dir():
            continue

        level = level_dir.name
        print(f"\n>>> Scanning Level {level}...")

        # 查找所有 MD 文件
        md_files = list(level_dir.glob("*.md"))
        if not md_files:
            print(f"    No .md files found in {level}")
            continue

        library_data[level] = []

        for md_file in sorted(md_files):
            try:
                book_info = process_book(md_file, level)
                if book_info:
                    library_data[level].append(book_info)
                    total_books += 1
            except Exception as e:
                print(f"    [Error] Failed to process {md_file.name}: {e}")

    # 生成索引
    if library_data:
        generate_library_json(library_data)

    print("\n" + "=" * 60)
    print(f"Processing complete! Total: {total_books} books")
    print("=" * 60)


if __name__ == "__main__":
    main()
