#!/usr/bin/env python
# -*- encoding: utf-8 -*-
version = "0.1.1"
"""
@文件    :pdm.py
@说明    :模拟IDM下载方式的PDM下载器，命令行脚本
@时间    :2025/12/27 11:06:03
@作者    :Akira_TL
@版本    :0.1.1
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
        # threads
    ):
        """
        初始化下载管理器。
        参数：
        - max_downloads: 全局并发下载任务数上限
        - timeout: 请求超时时间（秒）
        - retry: 失败重试次数
        - retry_wait: 重试之间的等待时间（秒）
        - log_path: 日志输出路径或 stdout
        - debug: 是否启用调试日志
        - check_integrity: 合并后校验完整性（MD5）
        - continue_download: 是否断点续传
        - max_concurrent_downloads: 单URL内部并发分片数
        - min_split_size: 自动切分最小分片大小（如“1M”）
        - force_sequential: 是否强制顺序下载
        - tmp_dir: 全局临时目录
        - user_agent: 用户代理字符串或字典
        - chunk_retry_speed: 低速阈值（B/s）低于则重启片段
        - chunk_timeout: 片段请求的超时（秒）
        - auto_file_renaming: 是否自动重命名避免覆盖
        - out_dir: 默认输出目录
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
        self.user_agent = user_agent  # or {"User-Agent": "PDM-Downloader/1.0"}
        self.check_integrity = check_integrity
        self.chunk_retry_speed = chunk_retry_speed
        self.retry_wait = retry_wait
        self.auto_file_renaming = auto_file_renaming
        self.out_dir = out_dir

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
        """
        解析并规范化管理器配置，设置日志、单位解析与并发限制。
        注意：会把 `min_split_size` 与 `chunk_retry_speed` 转换为字节数。
        """
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
        """
        将类似 '1M'、'256K' 或纯数字字符串解析为字节数。
        返回 None 表示未设置。
        """
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
        """
        批量添加下载任务。支持：
        - dict: {url: {md5, file_name, dir_path, log_path}}
        - list[str]: [url1, url2, ...]
        """
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
        """
        从文件中加载URL列表，支持 JSON / YAML / 纯文本（每行一个URL），并合并到任务队列。
        """
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
        """
        添加单个下载任务，指定保存目录/文件名/日志/校验等。
        """
        self._urls[url] = PDManager.FileDownloader(
            self, url, dir_path, filename=file_name, md5=md5, log_path=log_path
        )
        self._logger.debug(f"Added URL: {url}")

    def pop(self, url: str):
        """
        从管理器的任务字典移除指定URL（若不存在则忽略）。
        """
        self._urls.pop(url, None)
        self._logger.debug(f"Removed URL: {url}")

    async def wait(self, downloaders: list[asyncio.Task]):
        """
        等待至少一个下载任务完成，清理完成的任务并记录错误。
        """
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
            # self.pop(_url)

    async def start_download(self):
        """
        启动全局下载循环：在进度面板下，按 `max_downloads` 并发度调度各 URL 下载器。
        当队列动态添加新任务时也会继续处理。
        """
        # >>> 全局调度循环说明
        # 1. 遍历 `_urls`，以不超过 `max_downloads` 的并发度启动 `FileDownloader.start_download()`
        # 2. 使用 `wait()` 在有任务完成后再补充新的任务
        # 3. 进度面板 `self._progress` 负责渲染每个文件的进度与合并阶段
        # <<<
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
        "返回当前待下载URL列表。"
        return list(self._urls.keys())

    def __str__(self):
        return f"PDManager(threads={self.max_downloads}, timeout={self.timeout}, retry={self.retry}, debug={self.debug}, continue_download={self.continue_download}, max_concurrent_downloads={self.max_concurrent_downloads}, min_split_size={self.min_split_size})"

    class FileDownloader:
        """
        单URL下载器：负责获取头信息、构建分片链表、并发下载、合并与校验。
        """

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
            """
            解析下载器配置：计算日志路径、临时目录、拉取头信息与文件名、确定文件大小，
            并构建或重建分片任务链表。
            """
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

        async def process_md5(self, md5):  # 处理传入的md5值
            """
            处理传入的 MD5 值：
            - 文件路径：读取文件内容作为 MD5
            - URL：请求获取文本作为 MD5
            - 32位十六进制：直接使用
            返回标准化的 MD5（小写）或 None。
            """
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
            """
            根据响应头 `Content-Disposition` 或 URL 路径推断文件名，
            当无法推断时以 URL hash 作为默认文件名。
            """
            async with aiohttp.ClientSession() as session:
                cd = self.header_info.get("Content-Disposition")
                if cd:
                    fname = re.findall('.*filename="*(.+)"*', cd)
                    # fname可能存在中文使用%xx编码的情况，需要解码
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
            """
            发送 HEAD 请求获取响应头；允许重定向，禁用内容压缩以便准确获取长度。
            """
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
            """
            从已获取的响应头解析 `Content-Length`，未知返回 -1。
            """
            if self.header_info is not None:
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
                chunk_size -= chunk_size % 10240  # 保证chunk_size是10K的整数倍
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
            """
            在当前分片链表中寻找进度间隙最大的片段，按中点切分生成新片段以提升并发。
            当最大可分间隙小于 `min_split_size` 时返回 None。
            """
            # >>> 间隙选择与切分策略
            # - gap = chunk.end - chunk.size - chunk.start + 1 表示剩余待下载字节
            # - new_start 取当前剩余与下一片段边界的中点，并对齐到 10KB
            # - 调整链表：当前片段缩短为 [start, new_start-1]，新片段覆盖 [new_start, old_end]
            # <<<
            async with self.lock:
                max_gap = 0
                target_chunk: "PDManager.FileDownloader.Chunk" = (
                    None  # 最大间隙目标片段
                )
                for chunk in self.chunk_root:
                    gap = chunk.end - chunk.size - chunk.start + 1  # 剩余待下载字节数
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
                    new_start -= (
                        new_start % 10240
                    )  # 对齐到10KB，提升服务器/内核处理效率
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
            """
            初始化或校验 `.pdm` 元信息文件，确保断点续传的参数一致性；
            不一致时清理并重建临时目录。
            """
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
            """
            逐片合并到目标文件（先写临时文件 `.tmp` 再原子替换），
            若目标文件存在且启用自动重命名，则生成递增后缀避免覆盖。
            合并过程在进度条中展示总字节数。
            """
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
            task = self.parent._progress.add_task(
                description=f"Merging {self.filename}",
                total=self.file_size if self.file_size > 0 else sum(self.chunk_root),
            )
            async with aiofiles.open(temp_path, "wb") as outfile:
                for chunk in self.chunk_root:
                    async with aiofiles.open(chunk.chunk_path, "rb") as infile:
                        while True:
                            data = await infile.read(64 * 1024)  # 64KB 缓冲
                            if not data:
                                break
                            self.parent._progress.update(task, advance=len(data))
                            await outfile.write(data)
            self.parent._progress.stop_task(task)

            await asyncio.to_thread(os.replace, temp_path, dest_path)
            await asyncio.to_thread(shutil.rmtree, self.pdm_tmp, True)

        async def check_integrity(self):
            """
            可选的 MD5 完整性校验：对合并后的文件计算 MD5 与期望值比对。
            """
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
            """
            包装整体下载流程：配置初始化 -> 并发分片下载 -> 合并 -> 校验。
            失败时按 `retry` 次数递归重试。
            """
            if _iter is None:
                _iter = self.parent.retry
            try:
                await self.parse_config()
                if (
                    self.filename is not None
                    and os.path.exists(os.path.join(self.filepath, self.filename))
                    and not self.parent.auto_file_renaming
                ):
                    self.parent.pop(self.url)
                    return self.url
                await self._start_download()
                await self.merge_chunks()
                await self.check_integrity()
                self._done = True
            except Exception as e:
                self._logger.debug(traceback.format_exc())
                await asyncio.sleep(self.parent.retry_wait)
                await self.start_download(_iter=_iter - 1) if _iter > 0 else None
            self.parent.pop(self.url)
            if self._done:
                return self.url
            else:
                raise Exception(f"Failed to download {self.url} after retries.")

        async def _start_download(self):
            """
            分片并发下载调度：控制同时进行的分片数不超过 `max_concurrent_downloads`。
            依据进度动态创建新分片，直至无需再切分。
            """
            # >>> 进度与并发调度说明
            # - `progress_run` 独立任务：未知大小时显示活动状态；已知大小时按合计字节更新进度
            # - 初始遍历现有分片，填满并发窗口；随后循环：等待任一任务完成后尝试 `create_chunk()`
            # - 当 `create_chunk()` 返回 None 表示无需进一步切分，最终等待所有任务完成
            # <<<
            tasks = []

            async def progress_run():
                if self.file_size < 0:  # rich 持续加载但无进度
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
                    # 任意一个任务完成后再添加新的任务
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
                    new_chunk = await self.create_chunk()  # TODO
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
                new_chunk = await self.create_chunk()  # TODO
                if new_chunk is None:
                    break
                tasks.append(asyncio.create_task(new_chunk.download()))
            await asyncio.gather(*tasks, self.progress)

        # >>> 分片链表说明
        # 每个 `Chunk` 表示文件的一个字节区间（闭区间），以单向链表串联，
        # 支持在下载过程中按剩余进度动态切分并插入新片段以提升并发度。
        # 片段完成条件：已写入字节数 `size` 达到区间长度（end-start+1）。
        # 对未知大小任务（end=None），响应结束即视为完成整个文件。
        # <<<
        class Chunk:
            """
            文件分片节点：形成单向链表结构以表示按顺序的字节区间。
            属性：
            - start/end: 区间起止（闭区间），未知大小时 `end=None`
            - size: 已写入的字节数（来自临时文件尺寸或运行中累计）
            - forward/next: 前驱与后继片段指针
            """

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

            def _is_complete(self) -> bool:
                "判断片段是否已下载完整。"
                return self.end is not None and self.size == self.end - self.start + 1

            def _needs_download(self) -> bool:
                "判断片段是否仍需继续下载。"
                return self.end is None or self.size < self.end - self.start + 1

            def _apply_range_header(self, headers: dict):
                "根据当前片段进度设置 Range 头；未知大小时移除 Range。"
                if self.end is not None:
                    headers["Range"] = f"bytes={self.start + self.size}-{self.end}"
                else:
                    if "Range" in headers:
                        headers.pop("Range")

            async def _stream_response(self, response, f) -> bool:
                """
                读取响应数据流并写入到临时分片文件：
                - 控制写入不超过片段 `end`
                - 统计瞬时速率，低于阈值时返回 True 以触发重启
                - 线程安全地累计 `size`
                返回：是否触发低速重启。
                """
                last_time = time.time()
                pos = await f.tell()  # 已写入偏移，避免覆盖与越界
                continue_flag = False
                async for data in response.content.iter_chunked(10240):
                    if self.end is not None:
                        remaining = self.end - self.start + 1 - pos
                        if remaining <= 0:
                            break
                        data = data[:remaining]  # 限制写入长度到片段边界
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
                            continue_flag = True  # 低速触发重启标记
                        last_time = now
                    pos += len(data)
                return continue_flag

            async def _split_incomplete(self):
                """
                将未完整的片段在当前进度处一分为二，以便继续并发下载。
                当前片段成为前半段，新片段承接后半段并插入到链表中。
                """
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
                """
                执行单片段下载的主循环：
                - 外层重试次数由管理器控制
                - 内层循环处理请求、写入与低速重启
                - 对未知大小的任务，在响应结束后标记文件级完成
                - 对已知大小的任务，必要时在结束处执行片段拆分
                """
                # >>> 分片下载流程概述
                # 1) 复用 `ClientSession` 与单次打开的临时文件句柄
                # 2) 若片段已完整则直接返回；否则构造 Range 并请求
                # 3) 流式写入 + 速率监测；低速则短暂等待并重启
                # 4) 已知大小：完成后返回；未知大小：响应结束即视为完成
                # <<<
                assert self.end is not None or self.size >= 0
                headers = {}
                file_mode = "ab" if os.path.exists(self.chunk_path) else "wb"
                async with aiohttp.ClientSession() as session, aiofiles.open(
                    self.chunk_path, file_mode
                ) as f:
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
        default="",
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
        default=None,
        help="Timeout in seconds for each download request.",
    )
    parser.add_argument(
        "--chunk-timeout",
        type=int,
        default=None,
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
        "--no-auto-file-renaming",
        action="store_false",
        help="Disable automatically renaming the output file if a file with the same name already exists in the target directory.",
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
        auto_file_renaming=args.no_auto_file_renaming,
        out_dir=args.dir,
    )
    if len(args.urls) == 1 and args.out is not None:
        pdm.append(
            args.urls[0],
            file_name=args.out,
        )
    else:
        if args.out is not None:
            Warning(
                "The --out option is only valid when downloading a single URL. Ignoring the --out option."
            )
        pdm.add_urls(
            args.urls,
        )
    if args.input_file:
        for file in args.input_file:
            if os.path.exists(file):
                pdm.load_input_file(file)
    asyncio.run(pdm.start_download())
