#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""
核心库模块：导出 PDManager 类供外部 import 使用。
保留原有行为与日志/并发策略；CLI 部分在 cli.py。
"""

import re
import os
import sys
import traceback
import yaml
import json
import time
import shutil
import asyncio
import hashlib
import aiohttp
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
    """
    下载管理器，负责全局配置、任务队列调度与进度显示。
    提供解析输入、创建下载器、并发控制与重试等能力。
    """

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
        max_concurrent_downloads: int = 5,
        min_split_size: str = "1M",
        force_sequential: bool = False,
        tmp_dir: str = None,
        user_agent: dict | str = None,
        chunk_retry_speed: str | int = None,
        chunk_timeout: int = 10,
        auto_file_renaming: bool = True,
        out_dir: str = None,
    ):
        """
        初始化下载管理器。
        参数：同原脚本 pdm.py。
        """
        self.max_downloads = max_downloads
        self.timeout = timeout
        self.chunk_timeout = chunk_timeout
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
        self.max_concurrent_downloads = max_concurrent_downloads
        self.min_split_size = min_split_size
        self.force_sequential = force_sequential
        self.tmp_dir = tmp_dir
        self.user_agent = user_agent
        self.check_integrity = check_integrity
        self.chunk_retry_speed = chunk_retry_speed
        self.retry_wait = retry_wait
        self.auto_file_renaming = auto_file_renaming
        self.out_dir = out_dir

        self._urls_lock = asyncio.Lock()
        self._urls: dict = {}
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

    def config(self, **kwargs):
        for k, v in kwargs.items():
            if hasattr(self, k) and k not in [
                "_urls",
                "_progress",
                "_logger",
                "_urls_lock",
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
        if isinstance(self.user_agent, str):
            try:
                self.user_agent = json.loads(self.user_agent)
            except Exception:
                self.user_agent = {"User-Agent": self.user_agent}

    def parse_size(self, size_str: str) -> int:
        if size_str is None or size_str == "":
            return None
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

    def add_urls(self, url_list: dict | list[str]):
        if type(url_list) == dict:
            for url, v in url_list.items():
                assert type(v) == dict
                self.append(
                    url,
                    md5=v.get("md5"),
                    file_name=v.get("file_name"),
                    dir_path=v.get(
                        "dir_path", self.out_dir if self.out_dir else os.getcwd()
                    ),
                    log_path=v.get("log_path", None),
                )
        else:
            for url in url_list:
                self.append(url, dir_path=self.out_dir if self.out_dir else os.getcwd())

    def load_input_file(self, input_file: str):
        with open(input_file, "r") as f:
            content = f.read()
            try:
                data = json.loads(content)
                self.add_urls(data)
            except json.JSONDecodeError:
                try:
                    data = yaml.safe_load(content)
                    self.add_urls(data)
                except Exception:
                    url_list = content.splitlines()
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
        async def _append_safe():
            async with self._urls_lock:
                self._urls[url] = PDManager.FileDownloader(
                    self, url, dir_path, filename=file_name, md5=md5, log_path=log_path
                )
            self._logger.debug(f"Added URL: {url}")

        # 支持在同步上下文中调用
        if asyncio.get_event_loop().is_running():
            return asyncio.create_task(_append_safe())
        else:
            asyncio.run(_append_safe())

    def pop(self, url: str):
        async def _pop_safe():
            async with self._urls_lock:
                self._urls.pop(url, None)
            self._logger.debug(f"Removed URL: {url}")

        if asyncio.get_event_loop().is_running():
            return asyncio.create_task(_pop_safe())
        else:
            asyncio.run(_pop_safe())

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
                self._logger.error(f"task error: {e}")
                self._logger.error(traceback.format_exc())
            downloaders.remove(d)

    async def start_download(self):
        self._logger.debug(self)
        downloaders = []
        downloading = {}
        with self._progress:

            while self._urls:
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
                await self.wait(downloaders)
                await asyncio.sleep(1)

    def urls(self) -> List[str]:
        return list(self._urls.keys())

    def __str__(self):
        return f"PDManager(threads={self.max_downloads}, timeout={self.timeout}, retry={self.retry}, debug={self.debug}, continue_download={self.continue_download}, max_concurrent_downloads={self.max_concurrent_downloads}, min_split_size={self.min_split_size})"

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
            self._downloaded = False
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
            self.header_info = await self.get_headers()
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

        async def process_md5(self, md5):
            if md5 is None:
                return None
            elif os.path.exists(md5):
                async with aiofiles.open(md5, "r") as f:
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
                cd = self.header_info.get("Content-Disposition")
                if cd:
                    fname = re.findall('.*filename="*(.+)"*', cd)
                    fname = unquote(fname[0]) if fname else None
                    if fname:
                        return fname
                fname = os.path.basename(URL(self.url).path)
                if fname == "":
                    fname = f"{hashlib.sha256(self.url.encode('utf-8')).hexdigest()[:6]}.dat"
                    self._logger.warning(
                        f"Cannot get filename from URL, use hash url as filename: {fname}"
                    )
                return fname

        async def get_headers(self) -> dict:
            async with aiohttp.ClientSession() as session:
                async with session.head(
                    self.url,
                    allow_redirects=True,
                    timeout=self.parent.timeout,
                    headers={"Accept-Encoding": "identity"},
                ) as response:
                    if response.status in (200, 206):
                        return response.headers
                    else:
                        raise Exception(
                            f"Failed to get header info, status code: {response.status},headers:{response.headers}"
                        )

        async def get_url_file_size(self) -> int:
            if self.header_info is not None:
                file_size = self.header_info.get("Content-Length")
            if file_size:
                return int(file_size)
            else:
                return -1

        def get_file_size(self) -> int:
            return self.file_size

        async def build_task(self):
            if self.file_size < 0:
                return PDManager.FileDownloader.Chunk(
                    self,
                    0,
                    None,
                    os.path.join(self.pdm_tmp, f"{self.filename}.0"),
                )
            chunk_size = self.file_size // self.parent.max_concurrent_downloads
            if chunk_size < self.parent.min_split_size:
                chunk_size = self.parent.min_split_size
            elif chunk_size // 10240:
                chunk_size -= chunk_size % 10240
            starts = list(range(0, self.file_size, chunk_size))
            if starts[-1] < self.parent.min_split_size and len(starts) > 1:
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
                if start >= self.file_size:
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
            file_list = {
                p.removeprefix(os.path.join(self.pdm_tmp, self.filename) + "."): p
                for p in glob(os.path.join(self.pdm_tmp, self.filename) + "*")
            }
            ordered_starts = sorted([int(k) for k in file_list.keys()])
            root = None
            if not ordered_starts:
                return root
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
            temp_path = dest_path + ".tmp"
            task = self.parent._progress.add_task(
                description=f"Merging {self.filename}",
                total=self.file_size if self.file_size > 0 else sum(self.chunk_root),
            )
            async with aiofiles.open(temp_path, "wb") as outfile:
                for chunk in self.chunk_root:
                    async with aiofiles.open(chunk.chunk_path, "rb") as infile:
                        while True:
                            data = await infile.read(64 * 1024)
                            if not data:
                                break
                            self.parent._progress.update(task, advance=len(data))
                            await outfile.write(data)
            self.parent._progress.stop_task(task)

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
                        data = await f.read(64 * 1024)
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
            if _iter is None:
                _iter = self.parent.retry
            try:
                await self.parse_config()
                if (
                    self.filename is not None
                    and os.path.exists(os.path.join(self.filepath, self.filename))
                    and not self.parent.auto_file_renaming
                ):
                    await self.parent.pop(self.url)
                    return self.url
                await self._start_download()
                await self.merge_chunks()
                await self.check_integrity()
                self._done = True
            except Exception as e:
                self._logger.debug(traceback.format_exc())
                await asyncio.sleep(self.parent.retry_wait)
                await self.start_download(_iter=_iter - 1) if _iter > 0 else None
            await self.parent.pop(self.url)
            if self._done:
                return self.url
            else:
                raise Exception(f"Failed to download {self.url} after retries.")

        async def _start_download(self):
            tasks = []

            async def progress_run():
                if self.file_size < 0:
                    task = self.parent._progress.add_task(
                        f"Downloading {self.filename}", total=None
                    )
                    while not self._downloaded:
                        await asyncio.sleep(1)
                else:
                    task = self.parent._progress.add_task(
                        f"Downloading {self.filename}", total=self.file_size
                    )
                    while self.file_size > sum(self.chunk_root):
                        self.parent._progress.update(
                            task, completed=sum(self.chunk_root)
                        )
                        await asyncio.sleep(1)
                    self.parent._progress.update(task, completed=sum(self.chunk_root))
                    self._logger.info(f"Completed downloading {self.filename}")
                self.parent._progress.stop_task(task)
                self.parent._progress.remove_task(task)

            self.progress = asyncio.create_task(progress_run())

            for chunk in self.chunk_root:
                if tasks.__len__() < self.parent.max_concurrent_downloads:
                    self._logger.debug(
                        f"tasks number {tasks.__len__()} < max_concurrent_downloads {self.parent.max_concurrent_downloads}, creating new task."
                    )
                    tasks.append(asyncio.create_task(chunk.download()))
                else:
                    self._logger.debug(
                        f"tasks number {tasks.__len__()} >= max_concurrent_downloads {self.parent.max_concurrent_downloads}, wait for a task to complete before creating new task."
                    )
                    done, pending = await asyncio.wait(
                        tasks, return_when=asyncio.FIRST_COMPLETED
                    )
                    for d in done:
                        tasks.remove(d)
                    tasks.append(asyncio.create_task(chunk.download()))
            while True:
                if tasks.__len__() < self.parent.max_concurrent_downloads:
                    self._logger.debug(
                        f"tasks number {tasks.__len__()} < max_concurrent_downloads {self.parent.max_concurrent_downloads}, creating new task."
                    )
                    new_chunk = await self.create_chunk()
                    if new_chunk is None:
                        break
                    tasks.append(asyncio.create_task(new_chunk.download()))
                    continue
                self._logger.debug(
                    f"tasks number {tasks.__len__()} >= max_concurrent_downloads {self.parent.max_concurrent_downloads}, wait for a task to complete before creating new task."
                )
                done, pending = await asyncio.wait(
                    tasks, return_when=asyncio.FIRST_COMPLETED
                )
                for d in done:
                    tasks.remove(d)
                new_chunk = await self.create_chunk()
                if new_chunk is None:
                    break
                tasks.append(asyncio.create_task(new_chunk.download()))
            await asyncio.gather(*tasks, self.progress)

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

            def _is_complete(self) -> bool:
                return self.end is not None and self.size == self.end - self.start + 1

            def _needs_download(self) -> bool:
                return self.end is None or self.size < self.end - self.start + 1

            def _apply_range_header(self, headers: dict):
                if self.end is not None:
                    headers["Range"] = f"bytes={self.start + self.size}-{self.end}"
                else:
                    if "Range" in headers:
                        headers.pop("Range")

            async def _stream_response(self, response, f) -> bool:
                last_time = time.time()
                pos = await f.tell()
                continue_flag = False
                async for data in response.content.iter_chunked(10240):
                    if self.end is not None:
                        remaining = self.end - self.start + 1 - pos
                        if remaining <= 0:
                            break
                        data = data[:remaining]
                    await f.write(data)
                    async with self.parent.lock:
                        self.size += len(data)
                        now = time.time()
                        elaps = max(now - last_time, 1e-6)
                        speed = len(data) / elaps
                        if (
                            self.parent.parent.chunk_retry_speed
                            and speed < self.parent.parent.chunk_retry_speed
                        ):
                            continue_flag = True
                        last_time = now
                    pos += len(data)
                return continue_flag

            async def _split_incomplete(self):
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

            async def download(self):
                assert self.end is not None or self.size >= 0
                headers = {}
                file_mode = "ab" if os.path.exists(self.chunk_path) else "wb"
                async with (
                    aiohttp.ClientSession() as session,
                    aiofiles.open(self.chunk_path, file_mode) as f,
                ):
                    for _ in range(self.parent.parent.retry):
                        if os.path.exists(self.chunk_path) and self._is_complete():
                            return self
                        while True:
                            try:
                                self._apply_range_header(headers)
                                self.parent._logger.debug(
                                    f"Downloading chunk: {self}, with headers: {headers}"
                                )
                                if self._needs_download():
                                    async with session.get(
                                        self.parent.url,
                                        headers=headers,
                                        timeout=self.parent.parent.chunk_timeout,
                                    ) as response:
                                        if response.status in (200, 206):
                                            continue_flag = await self._stream_response(
                                                response, f
                                            )
                                            if continue_flag:
                                                await asyncio.sleep(
                                                    self.parent.parent.retry_wait
                                                )
                                                self.parent._logger.debug(
                                                    "speed is low restarting..."
                                                )
                                                break
                            except aiohttp.client_exceptions.ClientPayloadError:
                                await asyncio.sleep(self.parent.parent.retry_wait)
                            except Exception as e:
                                self.parent._logger.debug(
                                    f"Error downloading chunk {self}: {e}"
                                )
                                await asyncio.sleep(self.parent.parent.retry_wait)
                                break
                            if self._is_complete():
                                return self
                        if self.end is None:
                            self.parent._logger.debug(
                                f"completed download chunk (unknown size): {self}"
                            )
                            self.parent._downloaded = True
                            return self
                        elif self._needs_download():
                            self.parent._logger.debug(
                                f"retrying download chunk: {self}"
                            )
                        else:
                            self.parent._logger.debug(
                                f"completed download chunk: {self}"
                            )
                await self._split_incomplete()
                return self
