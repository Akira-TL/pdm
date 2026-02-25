#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""
核心库模块：导出 Manager 类供外部 import 使用。
保留原有行为与日志/并发策略；CLI 部分在 cli.py。
"""

import random
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
from .chunk import Chunk
from .downloader import Downloader
from .utils import auto_sync


class Manager:
    """
    ### 负责管理下载任务，提供添加 URL、启动下载、停止下载等功能。

    args:
        max_downloads: 最大下载**任务**数
        timeout: 下载超时时间，单位秒
        retry: 下载失败重试次数
        retry_wait: 下载失败重试等待时间，单位秒
        log_path: 日志输出路径或对象
        debug: 是否启用调试模式
        check_integrity: 是否启用完整性校验
        continue_download: 是否启用断点续传
        max_concurrent_downloads: 最大并发下载数
        min_split_size: 最小分块大小，单位支持 K/M/G
        force_sequential: 是否强制顺序下载
        tmp_dir: 临时文件目录
        user_agent: HTTP 请求的 User-Agent 字段
        chunk_retry_speed: 分块下载重试速度阈值，单位支持 K/M/G
        chunk_timeout: 分块下载超时时间，单位秒
        auto_file_renaming: 是否启用自动文件重命名以避免冲突
        out_dir: 下载文件输出目录，默认为当前工作目录

    attributes:
        config: 更新配置项的方法
        add_urls: 添加下载 URL 的方法，支持列表或字典格式
        load_input_file: 从文件加载下载 URL 的方法，支持 JSON/YAML/纯文本格式
        append: 添加单个下载 URL 的方法，支持指定文件名、目录和日志路径
        pop: 移除下载 URL 的方法
        wait: 等待下载任务完成的方法
        download: 启动下载的方法
        start_download: 启动持续下载的方法
        stop_download: 停止持续下载的方法
        urls: 获取当前下载 URL 列表的方法
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
        self._urls: dict = {}  # {url: Downloader item, ...}
        """{url: `Downloader` item, ...}"""
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
        self._downloader_main = None
        self._downloaders = []
        self._parse_config()

    def config(self, **kwargs):
        for k, v in kwargs.items():
            if hasattr(self, k) and not k.startswith("_"):
                setattr(self, k, v)
        self._parse_config()

    def _parse_config(self) -> None:
        """
        ### 解析配置项，处理日志设置、并发限制、大小单位转换等逻辑。

        args:
            self: Manager 实例

        returns:
            None

        """
        # >>> 解析日志设置
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
        # <<< 解析日志设置
        # >>> 解析下载参数
        # 并发下载任务配置
        self.max_downloads = int(self.max_downloads)
        if self.max_downloads < 1:
            self.max_downloads = 1
            self._logger.warning("threads cannot be less than 1. Setting to 1.")
        elif self.max_downloads > 32:
            self._logger.warning(
                "threads are more than 32, may cause high resource usage. "
            )
        # 单任务并发限制
        if self.max_concurrent_downloads < 1:
            self.max_concurrent_downloads = 1
            self._logger.warning(
                "max_concurrent_downloads cannot be less than 1. Setting to 1."
            )
        elif self.max_concurrent_downloads > 32:
            self._logger.warning(
                "max_concurrent_downloads is more than 32, becareful of server limits. "
            )
        # 数值转换
        self.min_split_size = self._parse_size(self.min_split_size)
        self.chunk_retry_speed = self._parse_size(self.chunk_retry_speed)
        if self.force_sequential:
            self.max_concurrent_downloads = 1
            self._logger.info("Force sequential download enabled.")
        # <<< 解析下载参数
        # >>> 解析 User-Agent 配置
        if isinstance(self.user_agent, str):
            try:
                self.user_agent = json.loads(self.user_agent)
            except Exception:
                self.user_agent = {"User-Agent": self.user_agent}
        # <<< 解析 User-Agent 配置

    def _parse_size(self, size_str: str) -> int:
        """
        ### 解析大小字符串，支持 K/M/G 单位，并转换为字节数。

        args:
            size_str: 大小字符串，例如 "1M", "500K", "2G"
        returns:
            int: 转换后的字节数，整数类型
        raises:
            ValueError: 如果输入格式不正确，抛出异常
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

    def add_urls(self, url_list: dict | list[str]) -> None:
        """
        添加多个 URL 到下载队列。

        args:
            url_list: 可以是字典或字符串列表。如果是字典，键为 URL，值为包含 md5、file_name、dir_path 和 log_path 的字典。
        returns:
            None
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

    def load_input_file(self, input_file: str) -> None:
        """
        加载输入文件并将其中的 URL 添加到下载队列。

        args:
            input_file: 输入文件路径，支持 JSON、YAML 或纯文本格式。
        returns:
            None
        """
        with open(input_file, "r") as f:
            content = f.read()
            endfix = os.path.splitext(input_file)[1].lower()
            assert type(content) == str
            match endfix:
                case ".json":
                    try:
                        data = json.loads(content)
                        self.add_urls(data)
                    except json.JSONDecodeError as e:
                        self._logger.error(f"Failed to parse JSON: {e}")
                case ".yaml" | ".yml":
                    try:
                        data = yaml.safe_load(content)
                        self.add_urls(data)
                    except yaml.YAMLError as e:
                        self._logger.error(f"Failed to parse YAML: {e}")
                case _:
                    url_list = content.splitlines()
                    url_list = [url.strip() for url in url_list if url.strip()]
                    self.add_urls(url_list)

    @auto_sync
    async def append(
        self,
        url: str,
        md5: str = None,
        file_name: str = None,
        dir_path: str = os.getcwd(),
        log_path: str = None,
    ) -> None:
        """
        添加单个 URL 到下载队列。
        args:
            url: 下载链接
            md5: 可选的 MD5 校验值
            file_name: 可选的文件名，默认为 URL 中的文件名
            dir_path: 可选的下载目录，默认为当前工作目录
            log_path: 可选的日志路径，默认为 None
        returns:
            None
        """
        async with self._urls_lock:
            self._urls[url] = Downloader(
                self, url, dir_path, filename=file_name, md5=md5, log_path=log_path
            )
            self._logger.debug(f"Added URL: {url}")

    @auto_sync
    async def pop(self, url: str) -> dict | None:
        async with self._urls_lock:
            result = self._urls.pop(url, None)
        self._logger.debug(f"Popped URL: {url}")
        return result

    @auto_sync
    async def popitem(self) -> tuple[str, dict] | None:
        async with self._urls_lock:
            if not self._urls:
                return None
            url, download_entity = self._urls.popitem()
        self._logger.debug(f"Popped URL item: {url}")
        return url, download_entity

    async def wait(self) -> None:
        while self._downloaders:
            await self.wait_one()

    async def wait_one(self) -> None:
        try:
            done, pending = await asyncio.wait(
                self._downloaders, return_when=asyncio.FIRST_COMPLETED
            )
        except Exception as e:
            self._logger.error(f"wait error: {e}")
            self._logger.error(traceback.format_exc())
            return
        for d in done:
            try:
                _url = d.result()
            except asyncio.CancelledError:
                pass
            except Exception as e:
                self._logger.error(f"task error: {e}")
                self._logger.error(traceback.format_exc())
        self._downloaders = pending

    async def download(self) -> None:
        """
        开始下载任务。
        """
        self._logger.debug(self)
        self._downloader_main = asyncio.create_task(self._download_once())
        try:
            await self._downloader_main
        finally:
            self._downloader_main = None

    async def _download_once(self, downloading=None) -> None:
        if downloading is None:
            downloading = {}
        with self._progress:
            while self._urls:
                url, download_entity = await self.popitem()
                if url in downloading:
                    continue
                downloading[url] = True
                assert isinstance(download_entity, Downloader)
                if len(self._downloaders) < self.max_downloads:
                    self._downloaders.append(
                        asyncio.create_task(download_entity.start_download())
                    )
                else:
                    await self.wait_one()
                    self._downloaders.append(
                        asyncio.create_task(download_entity.start_download())
                    )
                self._logger.debug(f"Starting download for {url}")
            await self.wait()
            await asyncio.sleep(0.1)

    async def _loop(self) -> None:
        self._logger.debug(self)
        downloading = {}
        while True:
            await self._download_once(downloading)
            await asyncio.sleep(1)

    async def start_loop(self) -> None:  # 持续下载
        """
        开始下载循环
        """
        if self._downloader_main is not None:
            self._logger.warning("Download loop is already running.")
            return
        else:
            self._downloader_main = asyncio.create_task(self._loop())

    def stop_loop(self) -> None:  # 停止持续下载
        """
        停止下载循环。
        """
        if self._downloader_main is not None:
            self._downloader_main.cancel()
            self._downloader_main = None

    def urls(self) -> List[str]:
        """
        获取当前下载 URL 列表。
        returns:
            List[str]: 当前下载 URL 列表
        """
        return list(self._urls.keys())

    def __str__(self):
        return f"Manager(threads={self.max_downloads}, timeout={self.timeout}, retry={self.retry}, debug={self.debug}, continue_download={self.continue_download}, max_concurrent_downloads={self.max_concurrent_downloads}, min_split_size={self.min_split_size})"

    def __del__(self):
        self.stop_loop()

    # 支持with语法
    async def __aenter__(self):
        await self.continue_download()
        return self

    def __enter__(self):
        asyncio.run(self.__aenter__())
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.wait()
        await self.stop_loop()

    def __exit__(self, exc_type, exc, tb):
        asyncio.run(self.__aexit__(exc_type, exc, tb))
