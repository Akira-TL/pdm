#!/usr/bin/env python
# -*- encoding: utf-8 -*-
version = "0.1.1"
f"""
@文件    :pdm.py
@说明    :模拟IDM下载方式的PDM下载器，命令行脚本
@时间    :2025/12/27 11:06:03
@作者    :Akira_TL
@版本    :{version}
"""

import re
import os
import sys
import yaml
import json
import time
import shutil
import asyncio
import hashlib
import aiohttp
import argparse
import aiofiles
from yarl import URL
from glob import glob
from rich.text import Text
from urllib.parse import unquote
from typing import List, Optional, TextIO
from loguru._logger import Logger, Core

from rich.progress import (
    Progress,
    Console,
    BarColumn,
    DownloadColumn,
    TransferSpeedColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TextColumn,
)


class PDManager:
    def __init__(
        self,
        max_downloads: int = 4,
        timeout: int = 60,
        retry: int = 3,
        retry_wait: int = 5,
        log_path: str | TextIO = sys.stdout,
        debug: bool = False,
        check_integrity: bool = False,
        continue_download: bool = False,
        input_file: str = None,
        max_concurrent_downloads: int = 5,
        min_split_size: str = "1M",
        force_sequential: bool = False,
        tmp_dir: str = None,
        user_agent: dict = None,
        chunk_retry_speed: str | int = -1,
        timeout_chunk: int = 10,
        auto_file_renaming: bool = True,
        # threads
    ):
        self.max_downloads = max_downloads
        self.timeout = timeout
        self.timeout_chunk = timeout_chunk
        self.retry = retry
        self.log_path = log_path
        self._logger = Logger(
            core=Core(),
            exception=None,
            depth=0,
            record=False,
            lazy=False,
            colors=False,
            raw=False,
            capture=True,
            patchers=[],
            extra={},
        )
        self.debug = debug
        self.continue_download = continue_download
        self.input_file = input_file
        self.max_concurrent_downloads = max_concurrent_downloads
        self.min_split_size = min_split_size
        self.force_sequential = force_sequential
        self.tmp_dir = tmp_dir
        self.user_agent = user_agent  # or {"User-Agent": "PDM-Downloader/1.0"}
        self.check_integrity = check_integrity
        self.chunk_retry_speed = chunk_retry_speed
        self.retry_wait = retry_wait
        self.auto_file_renaming = auto_file_renaming

        self._dict_lock = asyncio.Lock()
        self._urls: dict = {}  # url:FileDownloader
        self._console = Console()
        self._progress = Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            DownloadColumn(binary_units=True),
            TransferSpeedColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=self._console,
        )
        self.parse_config()

    def config(self, **kwargs):  # 动态配置参数
        for k, v in kwargs.items():
            if hasattr(self, k) and k not in [
                "_urls",
                "_progress",
                "_logger",
                "_dict_lock",
            ]:
                setattr(self, k, v)
        self.parse_config()

    def parse_config(self):
        self._logger.remove()
        self._logger.add(
            lambda msg: self._console.print(Text.from_ansi(str(msg)), end="\n"),
            level="DEBUG" if self.debug else "INFO",
            diagnose=True,
            colorize=True,
            format="<g>{time:MM-DD HH:mm:ss}</g> [<lvl>{level}</lvl>] <c><u>{name}</u></c> | {message}",
        )
        if isinstance(self.log_path, str):
            self._logger.add(
                self.log_path,
                level="DEBUG" if self.debug else "INFO",
                diagnose=True,
                colorize=True,
                format="<g>{time:MM-DD HH:mm:ss}</g> [<lvl>{level}</lvl>] <c><u>{name}</u></c> | {message}",
            )
        if self.max_concurrent_downloads < 1:
            self.max_concurrent_downloads = 1
            self._logger.warning(
                "max_concurrent_downloads cannot be less than 1. Setting to 1."
            )
        elif self.max_concurrent_downloads > 32:
            self._logger.warning(
                "max_concurrent_downloads is more than 32, becareful of server limits. "
            )
        self.min_split_size = self.parse_size(self.min_split_size)
        self.chunk_retry_speed = self.parse_size(self.chunk_retry_speed)
        if self.force_sequential:
            self.max_concurrent_downloads = 1
            self._logger.info("Force sequential download enabled.")
        self.max_downloads = int(self.max_downloads)
        if self.max_downloads < 1:
            self.max_downloads = 1
            self._logger.warning("threads cannot be less than 1. Setting to 1.")
        elif self.max_downloads > 32:
            self._logger.warning(
                "threads are more than 32, may cause high resource usage. "
            )

    def parse_size(self, size_str: str) -> int:
        size_str = str(size_str).strip().upper()
        size_map = {"K": 1024, "M": 1024**2, "G": 1024**3}
        size = 1
        for i in range(len(size_str) - 1, -1, -1):
            unit = size_str[i]
            if unit in size_map:
                size *= size_map[unit]
                continue
            break
        num = size_str[: i + 1]
        if re.match(r"^\d+(\.\d+)?$", num):
            return int(float(num) * size)
        else:
            raise ValueError(f"Invalid size format: {size_str}")

    class FileDownloader:
        def __init__(
            self,
            parent,
            url,
            filepath,
            filename: str = None,
            md5=None,
            pdm_tmp=None,
            log_path=None,
        ):
            self.parent: PDManager = parent
            self.url = url
            self.filepath = filepath
            self.filename = filename
            self.md5 = md5
            self.pdm_tmp = pdm_tmp
            self.file_size: int = 0
            self.chunk_root: "PDManager.FileDownloader.Chunk" | None = None
            self.lock = asyncio.Lock()
            self.header_info = None
            self.log_path = log_path
            self._done = False
            self._logger = Logger(
                core=Core(),
                exception=None,
                depth=0,
                record=False,
                lazy=False,
                colors=False,
                raw=False,
                capture=True,
                patchers=[],
                extra={},
            )

        async def parse_config(self):
            sha = hashlib.sha256(self.url.encode("utf-8")).hexdigest()[:6]
            if self.log_path is None and self.parent.log_path is not None:
                self.log_path = os.path.join(self.filepath, f".pdm.{sha}.log")
            self._logger.remove()
            self._logger.add(
                lambda msg: self.parent._console.print(
                    Text.from_ansi(str(msg)), end="\n"
                ),
                level="DEBUG" if self.parent.debug else "INFO",
                diagnose=True,
                colorize=True,
                format="<g>{time:MM-DD HH:mm:ss}</g> [<lvl>{level}</lvl>] <c><u>{name}</u></c> | {message}",
            )
            if self.log_path is not None:
                self._logger.add(
                    self.log_path,
                    level="DEBUG" if self.parent.debug else "INFO",
                    diagnose=True,
                    colorize=True,
                    format="<g>{time:MM-DD HH:mm:ss}</g> [<lvl>{level}</lvl>] <c><u>{name}</u></c> | {message}",
                )
            if self.md5 is not None:
                if self.md5.find("*") == 0:
                    self.md5 = self.md5.replace("*", self.url)
                self.md5 = await self.process_md5(self.md5)
                self._logger
            if self.pdm_tmp is None and self.parent.tmp_dir is not None:
                self.pdm_tmp = os.path.join(self.parent.tmp_dir, f".pdm.{sha}")
            elif self.pdm_tmp is None and self.parent.tmp_dir is None:
                self.pdm_tmp = os.path.join(self.filepath, f".pdm.{sha}")
            else:
                self.pdm_tmp = os.path.join(self.pdm_tmp, f".pdm.{sha}")
            os.makedirs(self.pdm_tmp, exist_ok=True)
            self.filename = (
                self.filename if self.filename else await self.get_file_name()
            )
            os.makedirs(self.filepath, exist_ok=True)
            self.file_size = self.file_size or await self.get_url_file_size()
            self.creat_info()
            self.chunk_root = await self.rebuild_task()
            if self.chunk_root is None:
                self.chunk_root = await self.build_task()

        def __str__(self):
            chunks = []
            for chunk in self.chunk_root:
                chunks.append(str(chunk))
            return f"FileDownloader(url={self.url}, filepath={self.filepath}, filename={self.filename}, md5={self.md5}, pdm_tmp={self.pdm_tmp}, file_size={self.file_size})\n{"\n".join(chunks)}"

        async def process_md5(self, md5):  # 处理传入的md5值
            if md5 is None:
                return None
            elif os.path.exists(self.filepath):
                async with aiofiles.open(self.filepath, "r") as f:
                    md5 = await f.read()
                    return md5.strip()
            elif re.match(r"^(http|https|ftp)://", md5):
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        md5, timeout=self.parent.timeout
                    ) as md5_response:
                        if md5_response.status == 200:
                            md5_value = await md5_response.text()
                            return md5_value.strip()
                        else:
                            self._logger.error(
                                f"Failed to fetch md5 from url: {md5}, status code: {md5_response.status}"
                            )
            elif len(md5) == 32 and re.match(r"^[a-fA-F0-9]{32}$", md5):
                return md5.lower()
            else:
                self._logger.error(f"Invalid md5 value: {md5}")
                return None

        async def get_file_name(self) -> str:
            async with aiohttp.ClientSession() as session:
                if self.header_info is None:
                    async with session.head(
                        self.url,
                        allow_redirects=True,
                        headers={"User-Agent": self.parent.user_agent},
                        timeout=self.parent.timeout,
                    ) as response:
                        self.header_info = response.headers
                cd = self.header_info.get("Content-Disposition")
                if cd:
                    fname = re.findall('.*filename="*(.+)"*', cd)
                    # fname可能存在中文使用%xx编码的情况，需要解码
                    fname = unquote(fname[0]) if fname else None
                    if fname:
                        return fname
                fname = os.path.basename(URL(response.url).path)
                if fname == "":
                    fname = f"{hashlib.sha256(self.url.encode('utf-8')).hexdigest()[:6]}.dat"
                    self._logger.warning(
                        f"Cannot get filename from URL, use hash url as filename: {fname}"
                    )
                return fname

        async def get_url_file_size(self) -> int:
            async with aiohttp.ClientSession() as session:
                if self.header_info is not None:
                    async with session.head(
                        self.url,
                        allow_redirects=True,
                        timeout=self.parent.timeout,
                    ) as response:
                        self.header_info = response.headers
                file_size = self.header_info.get("Content-Length")
                if file_size:
                    return int(file_size)
                else:
                    return -1  # -1标记未知大小，与None区分

        def get_file_size(self) -> int:
            return self.file_size

        async def build_task(self):
            """
            新建分块链表
            Returns:
                PDManager.FileDownloader.ChunkManager: 分块链表头
            """
            chunk_size = (
                self.file_size // self.parent.max_concurrent_downloads
            )  # TODO file_size为-1时的处理
            if chunk_size < self.parent.min_split_size:
                chunk_size = self.parent.min_split_size
            elif chunk_size // 10240:
                chunk_size -= chunk_size % 10240  # 保证chunk_size是10K的整数倍
            starts = list(range(0, self.file_size, chunk_size))
            if starts[-1] < self.parent.min_split_size:
                starts.pop()
            root = None
            for i in range(len(starts)):
                start = starts[i]
                end = starts[i + 1] - 1 if i + 1 < len(starts) else self.file_size - 1
                if root is None:
                    root = cur = PDManager.FileDownloader.Chunk(
                        self,
                        start,
                        end,
                        os.path.join(self.pdm_tmp, f"{self.filename}.{start}"),
                    )
                    continue
                if start >= self.file_size:  # TODO 没有问题就移除
                    self._logger.warning(
                        f"start {start} >= file_size {self.file_size}, break"
                    )
                    break
                cur.next = PDManager.FileDownloader.Chunk(
                    self,
                    start,
                    end,
                    os.path.join(self.pdm_tmp, f"{self.filename}.{start}"),
                )
                cur = cur.next
            assert root is not None
            return root

        async def rebuild_task(self):
            """
            根据已有的分块文件重建分块链表
            Returns:
                PDManager.FileDownloader.ChunkManager: 分块链表头
            """
            file_list = {
                p.removeprefix(os.path.join(self.pdm_tmp, self.filename) + "."): p
                for p in glob(os.path.join(self.pdm_tmp, self.filename) + "*")
            }
            ordered_starts = sorted([int(k) for k in file_list.keys()])
            root = None
            if not ordered_starts:
                return root  # None
            for i in range(len(ordered_starts)):
                start = ordered_starts[i]
                end = (
                    ordered_starts[i + 1] - 1
                    if i + 1 < len(ordered_starts)
                    else self.file_size - 1
                )
                if root is None:
                    root = cur = PDManager.FileDownloader.Chunk(
                        self, start, end, file_list[str(start)]
                    )
                    continue
                cur.next = PDManager.FileDownloader.Chunk(
                    self, start, end, file_list[str(start)], cur
                )
                cur = cur.next
            return root

        async def create_chunk(self) -> "PDManager.FileDownloader.Chunk" | None:
            # 遍历chunk列表，先找到间隔最大的正在下载的chunk，然后在其间隙中创建新的chunk，间隔小于102400则返回None
            async with self.lock:
                max_gap = 0
                target_chunk: "PDManager.FileDownloader.Chunk" = None
                for chunk in self.chunk_root:
                    gap = chunk.end - chunk.size - chunk.start + 1
                    if gap > max_gap:
                        max_gap = gap
                        target_chunk = chunk
                if target_chunk is None or max_gap <= self.parent.min_split_size:
                    return None
                new_start = (
                    target_chunk.start
                    + target_chunk.size
                    + (
                        target_chunk.next.start
                        if target_chunk.next
                        else target_chunk.end
                    )
                ) // 2
                if new_start // 10240:
                    new_start -= new_start % 10240
                new_chunk = PDManager.FileDownloader.Chunk(
                    self,
                    new_start,
                    (
                        target_chunk.next.start - 1
                        if target_chunk.next
                        else target_chunk.end
                    ),
                    os.path.join(self.pdm_tmp, f"{self.filename}.{new_start}"),
                    target_chunk,
                    next=target_chunk.next,
                )
                new_chunk.end = target_chunk.end
                target_chunk.end = new_start - 1
                target_chunk.next = new_chunk
            return new_chunk

        def creat_info(self):
            if not self.parent.continue_download or not os.path.exists(
                os.path.join(self.pdm_tmp, ".pdm")
            ):
                shutil.rmtree(self.pdm_tmp, ignore_errors=True)
                os.makedirs(self.pdm_tmp, exist_ok=True)
                with open(os.path.join(self.pdm_tmp, ".pdm"), "w") as f:
                    info = {
                        "url": self.url,
                        "filename": self.filename,
                        "md5": self.md5,
                        "file_size": self.file_size,
                    }
                    json.dump(info, f, indent=4)
            elif os.path.exists(os.path.join(self.pdm_tmp, ".pdm")):
                with open(os.path.join(self.pdm_tmp, ".pdm"), "r") as f:
                    info = json.load(f)
                    if (
                        info.get("md5") != self.md5
                        or info.get("file_size") != self.file_size
                        or info.get("filename") != self.filename
                        or info.get("url") != self.url
                    ):
                        self._logger.warning(
                            "Existing .pdm file info does not match current download info, recreating .pdm file."
                        )
                        shutil.rmtree(self.pdm_tmp)
                        os.makedirs(self.pdm_tmp, exist_ok=True)
                        with open(os.path.join(self.pdm_tmp, ".pdm"), "w") as f:
                            info = {
                                "url": self.url,
                                "filename": self.filename,
                                "md5": self.md5,
                                "file_size": self.file_size,
                            }
                            json.dump(info, f, indent=4)
            else:
                self._logger.error("Unknown error in creating .pdm file.")

        async def merge_chunks(self):
            if os.path.exists(os.path.join(self.filepath, self.filename)):
                index = 0
                while True:
                    index += 1
                    if not os.path.exists(
                        os.path.join(self.filepath, f"{self.filename}.{index}")
                    ):
                        self.filename = f"{self.filename}.{index}"
                        break
            dest_path = os.path.join(self.filepath, self.filename)
            temp_path = dest_path + ".tmp"  # 先写入临时文件，最后原子替换
            async with aiofiles.open(temp_path, "wb") as outfile:
                for chunk in self.chunk_root:
                    async with aiofiles.open(chunk.chunk_path, "rb") as infile:
                        while True:
                            data = await infile.read(64 * 1024)  # 64KB 缓冲
                            if not data:
                                break
                            await outfile.write(data)

            await asyncio.to_thread(os.replace, temp_path, dest_path)
            await asyncio.to_thread(shutil.rmtree, self.pdm_tmp, True)

        async def check_integrity(self):
            if self.parent.check_integrity:
                if self.md5 is None:
                    self._logger.info(
                        f"{self.filename} No md5 provided, skipping integrity check."
                    )
                    return True
                dest_path = os.path.join(self.filepath, self.filename)
                hash_md5 = hashlib.md5()
                async with aiofiles.open(dest_path, "rb") as f:
                    while True:
                        data = await f.read(64 * 1024)  # 64KB 缓冲
                        if not data:
                            break
                        hash_md5.update(data)
                file_md5 = hash_md5.hexdigest()
                if file_md5.lower() == self.md5.lower():
                    self._logger.info(
                        f"{self.filename} MD5 checksum matches, integrity check passed."
                    )
                    return True
                else:
                    self._logger.error(
                        f"{self.filename}MD5 checksum does not match! Expected: {self.md5}, Got: {file_md5}"
                    )
                    return False

        async def start_download(self, _iter=None):
            if (
                os.path.exists(os.path.join(self.filepath, self.filename))
                and self.parent.auto_file_renaming
            ):
                return self.url
            if _iter is None:
                _iter = self.parent.retry
            try:
                await self.parse_config()
                await self._start_download()
                await self.merge_chunks()
                await self.check_integrity()
                self._done = True
            except Exception as e:
                await asyncio.sleep(self.parent.parent.retry_wait)
                await self.start_download(_iter=_iter - 1) if _iter > 0 else None
            if self._done:
                return self.url
            else:
                raise Exception(f"Failed to download {self.url} after retries.:{e}")

        async def _start_download(self):
            tasks = []

            async def progress_run():
                task = self.parent._progress.add_task(
                    f"Downloading {self.filename}", total=self.file_size
                )  # TODO
                while self.file_size > sum(self.chunk_root):
                    self.parent._progress.update(task, completed=sum(self.chunk_root))
                    await asyncio.sleep(1)
                self.parent._progress.remove_task(task)
                self._logger.info(f"Completed downloading {self.filename}")

            self.progress = asyncio.create_task(progress_run())

            for chunk in self.chunk_root:
                if tasks.__len__() < self.parent.max_concurrent_downloads:
                    self._logger.debug(
                        f"tasks number {tasks.__len__()} < max_concurrent_downloads {self.parent.max_concurrent_downloads}, creating new task."
                    )
                    tasks.append(asyncio.create_task(chunk.download()))
                else:
                    # 任意一个任务完成后再添加新的任务
                    done, pending = await asyncio.wait(
                        tasks, return_when=asyncio.FIRST_COMPLETED
                    )
                    for d in done:
                        tasks.remove(d)
                    self._logger.debug(
                        f"tasks number {tasks.__len__()} >= max_concurrent_downloads {self.parent.max_concurrent_downloads}, wait for a task to complete before creating new task."
                    )
                    tasks.append(asyncio.create_task(chunk.download()))
            while True:
                if tasks.__len__() < self.parent.max_concurrent_downloads:
                    self._logger.debug(
                        f"tasks number {tasks.__len__()} < max_concurrent_downloads {self.parent.max_concurrent_downloads}, creating new task."
                    )
                    new_chunk = await self.create_chunk()  # TODO
                    if new_chunk is None:
                        break
                    tasks.append(asyncio.create_task(new_chunk.download()))
                    continue
                done, pending = await asyncio.wait(
                    tasks, return_when=asyncio.FIRST_COMPLETED
                )
                for d in done:
                    tasks.remove(d)
                self._logger.debug(
                    f"tasks number {tasks.__len__()} >= max_concurrent_downloads {self.parent.max_concurrent_downloads}, wait for a task to complete before creating new task."
                )
                new_chunk = await self.create_chunk()  # TODO
                if new_chunk is None:
                    break
                tasks.append(asyncio.create_task(new_chunk.download()))
            await asyncio.gather(*tasks)
            await self.progress

        class Chunk:
            def __init__(
                self,
                parent: PDManager.FileDownloader,
                start: int,
                end: int,
                chunk_path: str,
                forward: "PDManager.FileDownloader.Chunk" = None,
                next: "PDManager.FileDownloader.Chunk" = None,
            ):
                self.parent = parent
                self.start = start
                self.end = end
                self.chunk_path = chunk_path
                if os.path.exists(chunk_path):
                    self.size = os.path.getsize(chunk_path)
                else:
                    self.size = 0
                self.forward: "PDManager.FileDownloader.Chunk" = forward
                self.next: "PDManager.FileDownloader.Chunk" = next

            def __iter__(self):
                current = self
                while current:
                    yield current
                    current = current.next

            def __str__(self):
                return f"Chunk(start={self.start}, end={self.end},target size={(self.end - self.start + 1) if self.end is not None else -1}, size={self.size}, chunk_path={self.chunk_path})"

            # 支持使用sum、+、-等
            def __add__(self, other):
                if not isinstance(other, PDManager.FileDownloader.Chunk):
                    return NotImplemented
                return self.size + other.size

            def __radd__(self, other):
                if other == 0:
                    return self.size
                if not isinstance(other, int):
                    return NotImplemented
                return self.size + other

            async def download(self):
                assert self.end is not None or self.size >= 0
                headers = {}
                # 复用会话
                async with aiohttp.ClientSession() as session:
                    # 仅打开一次文件（追加或新建）
                    file_mode = "ab" if os.path.exists(self.chunk_path) else "wb"
                    async with aiofiles.open(self.chunk_path, file_mode) as f:
                        # 断连续传
                        for _ in range(self.parent.parent.retry):
                            # 已完整下载
                            if os.path.exists(self.chunk_path):
                                if (
                                    self.end is not None
                                    and self.size == self.end - self.start + 1
                                ):
                                    return self
                            while True:
                                continue_flug = False
                                try:
                                    # 构造 Range（仅当 end 可用时）
                                    if self.end is not None:
                                        headers["Range"] = (
                                            f"bytes={self.start + self.size}-{self.end}"
                                        )
                                    else:
                                        if "Range" in headers:
                                            headers.pop("Range")
                                    self.parent._logger.debug(
                                        f"Downloading chunk: {self}, with headers: {headers}"
                                    )
                                    # 仅在未完成时下载（若 end 不可用则一直下载直到流结束）
                                    need_download = (
                                        self.end is None
                                        or self.size < self.end - self.start + 1
                                    )
                                    if need_download:
                                        async with session.get(
                                            self.parent.url,
                                            headers=headers,
                                            timeout=self.parent.parent.chunk_timeout,
                                        ) as response:
                                            if response.status in (200, 206):
                                                last_time = time.time()
                                                # 若文件已存在，定位到当前末尾
                                                pos = await f.tell()
                                                # 限制写入至 chunk.end（如果已知）
                                                async for (
                                                    data
                                                ) in response.content.iter_chunked(
                                                    10240
                                                ):  # 如果数据不足10240字节则直接写入
                                                    if self.end is not None:
                                                        remaining = (
                                                            self.end
                                                            - self.start
                                                            + 1
                                                            - pos
                                                        )
                                                        if remaining <= 0:
                                                            break
                                                        data = data[:remaining]
                                                    await f.write(data)
                                                    async with self.parent.lock:
                                                        self.size += len(data)
                                                        now = time.time()
                                                        # 计算速率，防止除以0
                                                        elaps = max(
                                                            now - last_time, 1e-6
                                                        )
                                                        speed = len(data) / elaps
                                                        # 如果速度低于阈值，则重启本片段下载
                                                        if (
                                                            speed
                                                            < self.parent.parent.chunk_retry_speed
                                                        ):
                                                            continue_flug = True
                                                        last_time = now
                                                        # self.parent.parent.progress.update(
                                                        #     self.parent.task,
                                                        #     advance=len(data),
                                                        # )
                                                    pos += len(data)
                                                if continue_flug:
                                                    await asyncio.sleep(
                                                        self.parent.parent.retry_wait
                                                    )
                                                    self.parent._logger.debug(
                                                        "speed is low restarting..."
                                                    )
                                                    break
                                except (
                                    aiohttp.client_exceptions.ClientPayloadError
                                ) as e:
                                    await asyncio.sleep(self.parent.parent.retry_wait)
                                except Exception as e:
                                    self.parent._logger.error(
                                        f"Error downloading chunk {self}: {e}"
                                    )
                                    await asyncio.sleep(self.parent.parent.retry_wait)
                                    break
                                if (
                                    self.end is not None
                                    and self.size == self.end - self.start + 1
                                ):
                                    return self
                            if self.end is None:
                                # 如果没有明确的 end，且循环结束则视为完成
                                self.parent._logger.debug(
                                    f"completed download chunk (unknown size): {self}"
                                )
                            elif self.size < self.end - self.start + 1:
                                self.parent._logger.debug(
                                    f"retrying download chunk: {self}"
                                )
                            else:
                                self.parent._logger.debug(
                                    f"completed download chunk: {self}"
                                )
                    if self.size != self.end - self.start + 1:
                        self.parent._logger.warning(
                            f"Chunk not fully downloaded, splitting chunk: {self}"
                        )
                        async with self.parent.lock:
                            new_start = self.start + self.size
                            new_chunk = PDManager.FileDownloader.Chunk(
                                self.parent,
                                new_start,
                                self.end,
                                os.path.join(
                                    self.parent.pdm_tmp,
                                    f"{self.parent.filename}.{new_start}",
                                ),
                                self,
                                next=self.next,
                            )
                            self.end = new_start - 1
                            self.next = new_chunk
                return self

    def add_urls(self, url_list: dict | list[str]):
        if type(url_list) == dict:
            for url, v in url_list.items():
                assert type(v) == dict
                self.append(
                    url,
                    md5=v.get("md5"),
                    file_name=v.get("file_name"),
                    dir_path=v.get("dir_path", os.getcwd()),
                    log_path=v.get("log_path", None),
                )
        else:
            for url in url_list:
                self.append(url)

    def load_input_file(self, input_file: str):
        with open(input_file, "r") as f:
            content = f.read()
            try:
                data = json.loads(content)  # 尝试json
                self.add_urls(data)
            except json.JSONDecodeError:
                try:
                    data = yaml.safe_load(content)  # 尝试yaml
                    self.add_urls(data)
                except Exception:
                    url_list = content.splitlines()  # 纯文本无额外字段
                    url_list = [url.strip() for url in url_list if url.strip()]
                    self.add_urls(url_list)

    def append(
        self,
        url: str,
        md5: str = None,
        file_name: str = None,
        dir_path: str = os.getcwd(),
        log_path: str = None,
    ):
        self._urls[url] = PDManager.FileDownloader(
            self, url, dir_path, filename=file_name, md5=md5, log_path=log_path
        )
        self._logger.debug(f"Added URL: {url}")

    def pop(self, url: str):
        self._urls.pop(url, None)
        self._logger.debug(f"Removed URL: {url}")

    async def wait(self, downloaders: list[asyncio.Task]):
        done, pending = await asyncio.wait(
            downloaders, return_when=asyncio.FIRST_COMPLETED
        )
        for d in done:
            try:
                _url = d.result()
            except asyncio.CancelledError:
                pass
            except Exception as e:
                self._logger.warning(f"task error: {e}")
            downloaders.remove(d)
            self.pop(_url)

    async def start_download(self):
        self._logger.debug(self)
        downloaders = []
        downloading = {}
        with self._progress:

            while self._urls:  # 如果在下载过程中添加了任务
                for url, download_entity in self._urls.items():
                    if url in downloading:
                        continue
                    downloading[url] = True
                    assert isinstance(download_entity, PDManager.FileDownloader)
                    if len(downloaders) < self.max_downloads:
                        downloaders.append(
                            asyncio.create_task(download_entity.start_download())
                        )
                    else:
                        await self.wait(downloaders)
                        downloaders.append(
                            asyncio.create_task(download_entity.start_download())
                        )
                    self._logger.debug(f"Starting download for {url}")
                # await asyncio.gather(*downloaders)
                await self.wait(downloaders)
                await asyncio.sleep(1)

    def urls(self) -> List[str]:
        return list(self._urls.keys())

    def __str__(self):
        return f"PDManager(threads={self.max_downloads}, timeout={self.timeout}, retry={self.retry}, debug={self.debug}, continue_download={self.continue_download}, input_file={self.input_file}, max_concurrent_downloads={self.max_concurrent_downloads}, min_split_size={self.min_split_size})"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version="pdm version 0.1.1",
        help="Print the version number and exit.",
    )
    parser.add_argument(
        "-l",
        "--log",
        type=str,
        required=False,
        default=None,
        help="The file name of the log file. If '-' is specified, log is written to stdout.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode with verbose logging.",
    )
    parser.add_argument(
        "-d",
        "--dir",
        type=str,
        default=os.path.join(os.getcwd(), "pdm"),
        help="The directory to store the downloaded file.",
    )
    parser.add_argument(
        "-o",
        "--out",
        type=str,
        default=None,
        help="The file name of the downloaded file. It is always relative to the directory given in -d option. When the -Z option is used, this option will be ignored.",
    )
    parser.add_argument(
        "-V",
        "--check-integrity",
        action="store_true",
        help="Check file integrity by validating piece hashes or a hash of the entire file. When piece hashes are provided, pdm can detect damaged portions and re-download them. When only a full-file hash is provided, the check is performed after the file appears to be fully downloaded; on mismatch, the file will be re-downloaded.",
    )
    parser.add_argument(
        "-c",
        "--continue",
        dest="continue_download",
        action="store_true",
        help="Continue downloading a partially downloaded file.",
    )
    parser.add_argument(
        "-i",
        "--input-file",
        type=str,
        default=[],
        action="append",
        help="Downloads URIs found in FILE(s). You can provide a JSON, YAML, or plain text file containing the list of URLs to download, along with optional metadata such as expected MD5 checksums, custom file names, and directory paths.",
    )
    parser.add_argument(
        "-x",
        "--max-concurrent-downloads",
        type=int,
        default=5,
        help="Set maximum number of parallel downloads for each URL or task.",
    )
    # 单个下载的子线程下载最小速度限制，低于限制重启下载线程,需要和单个下载的速率限制区分
    parser.add_argument(
        "--chunk-retry-speed",
        type=str,
        default=-1,
        help="If the download speed of a chunk falls below SIZE bytes/second, pdm will restart downloading that chunk (Excluded from retry). You can append K or M (1K = 1024, 1M = 1024K).",
    )
    parser.add_argument(
        "-r",
        "--retry",
        type=int,
        default=3,
        help="Number of times to retry downloading a URL upon failure.",
    )
    parser.add_argument(  # --retry-wait
        "-W",
        "--retry-wait",
        type=int,
        default=5,
        help="Maximum wait time in seconds between retries.",
    )
    parser.add_argument(  # timeout
        "--timeout",
        type=int,
        default=60,
        help="Timeout in seconds for each download request.",
    )
    parser.add_argument(
        "--chunk-timeout",
        type=int,
        default=10,
        help="Timeout in seconds for each chunk download request.",
    )
    parser.add_argument(
        "-N",
        "--max-downloads",
        type=int,
        default=4,
        help="The maximum number of concurrent downloads.",
    )
    parser.add_argument(
        "--auto-file-renaming",
        type=bool,
        default=True,
        help="Automatically rename the output file if a file with the same name already exists in the target directory.",
    )
    parser.add_argument(
        "-Z",
        "--force-sequential",
        action="store_true",
        help="Fetch URIs in the command-line sequentially and download each URI in a separate session, like usual command-line download utilities.",
    )
    parser.add_argument(
        "-k",
        "--min-split-size",
        type=str,
        default="1M",
        help="pdm will not split ranges smaller than 2*SIZE bytes. For example, for a 20MiB file: if SIZE is 10M, pdm can split into two ranges and use 2 sources (if --split >= 2). If SIZE is 15M, since 2*15M > 20MiB, pdm will not split the file and downloads using 1 source. You can append K or M (1K = 1024, 1M = 1024K).",
    )
    parser.add_argument(
        "--tmp",
        type=str,
        default=None,
        help="The temporary directory to store the downloading chunks.",
    )
    parser.add_argument(
        "-t",
        "--threads",
        type=int,
        default=4,
        help="The number of threads to use for downloading.",
    )
    parser.add_argument(
        "-ua",
        "--user-agent",
        type=str,
        default="PDM-Downloader/1.0",
        help="The User-Agent string to use for HTTP requests.",
    )
    parser.add_argument(
        "urls",
        type=str,
        nargs="*",
        default=None,
        help="The URL to download.",
    )  # 可以接受多个url参数

    args = parser.parse_args()
    if args.log == "-":
        args.log = sys.stdout
    if args.force_sequential and args.out is not None:
        Warning(
            "The --force-sequential option is used, the --out option will be ignored."
        )
        args.out = None

    pdm = PDManager(
        max_downloads=args.max_downloads,
        log_path=args.log,
        debug=args.debug,
        continue_download=args.continue_download,
        input_file=args.input_file,
        max_concurrent_downloads=args.max_concurrent_downloads,
        min_split_size=args.min_split_size,
        force_sequential=args.force_sequential,
        tmp_dir=args.tmp,
        check_integrity=args.check_integrity,
        user_agent=args.user_agent,
        chunk_retry_speed=args.chunk_retry_speed,
        retry=args.retry,
        retry_wait=args.retry_wait,
        timeout=args.timeout,
        chunk_timeout=args.chunk_timeout,
        auto_file_renaming=args.auto_file_renaming,
    )
    if args.urls:
        pdm.add_urls(
            args.urls,
        )
    if args.input_file:
        for file in args.input_file:
            if os.path.exists(file):
                pdm.load_input_file(file)
    asyncio.run(pdm.start_download())
