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

import argparse
from glob import glob
import hashlib
import json
import os, aiofiles
import time
import shutil
import subprocess
import sys
from typing import List, Optional, TextIO
import aiohttp
from loguru._logger import Logger, Core

from multidict import CIMultiDictProxy
import requests
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
from rich.text import Text

# console = Console()
from yarl import URL
import re
import asyncio

# logger = Logger(
#     core=Core(),
#     exception=None,
#     depth=0,
#     record=False,
#     lazy=False,
#     colors=False,
#     raw=False,
#     capture=True,
#     patchers=[],
#     extra={},
# )
# logger.remove()
# logger.add(
#     lambda msg: console.print(Text.from_ansi(str(msg)), end="\n"),
#     level="DEBUG",
#     diagnose=True,
#     colorize=True,
#     format="<g>{time:MM-DD HH:mm:ss}</g> [<lvl>{level}</lvl>] <c><u>{name}</u></c> | {message}",
# )


class PDManager:
    def __init__(
        self,
        threads: int = 4,
        timeout: int = 10,
        retry: int = 3,
        log_path: str | TextIO = sys.stdout,
        debug: bool = False,
        split: int = 5,
        #  check_integrity:bool=False,
        continue_download: bool = False,
        input_file: str = None,
        max_concurrent_downloads: int = 5,
        min_split_size: str = "20M",
    ):
        self.threads = threads
        self.timeout = timeout
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
        self.split = split
        # self.check_integrity = check_integrity
        self.continue_download = continue_download
        self.input_file = input_file
        self.max_concurrent_downloads = max_concurrent_downloads
        self.min_split_size = min_split_size

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
            self.max_concurrent_downloads = 32
            self._logger.warning(
                "max_concurrent_downloads cannot be more than 32. Setting to 32."
            )
        self.min_split_size = self.parse_size(self.min_split_size)

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
                self.md5 = await self.process_md5(self.md5)
                self._logger
            if self.pdm_tmp is None:
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
                    async with session.get(md5) as md5_response:
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
                    async with session.head(self.url, allow_redirects=True) as response:
                        self.header_info = response.headers
                cd = self.header_info.get("Content-Disposition")
                if cd:
                    fname = re.findall('filename="(.+)"', cd)
                    if fname:
                        return fname[0]
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
                    async with session.head(self.url, allow_redirects=True) as response:
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
                self.file_size // self.parent.split
            )  # TODO file_size为-1时的处理
            if chunk_size < self.parent.min_split_size:
                chunk_size = self.parent.min_split_size
            elif chunk_size // 10240:
                chunk_size -= chunk_size % 10240  # 保证chunk_size是10K的整数倍
            starts = list(range(0, self.file_size, chunk_size))
            if starts[-1] < 102400:
                starts[-2] += starts[-1]
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
                if target_chunk is None or max_gap <= 10240:
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

        async def start_download(self):
            await self.parse_config()
            await self._start_download()
            await self.merge_chunks()
            return self.url

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

            asyncio.create_task(progress_run())

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
                return f"Chunk(start={self.start}, end={self.end},target size={(self.end - self.start + 1) if self.end is not None else -1}, size={self.size}, , chunk_path={self.chunk_path})"

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
                                if self.size == self.end - self.start + 1:
                                    return self
                            try:
                                # 构造 Range
                                headers["Range"] = (
                                    f"bytes={self.start + self.size}-{self.end}"
                                )
                                self.parent._logger.debug(
                                    f"Downloading chunk: {self}, with headers: {headers}"
                                )
                                # 仅在未完成时下载
                                if self.size < self.end - self.start + 1:
                                    async with session.get(
                                        self.parent.url, headers=headers
                                    ) as response:
                                        if response.status in (200, 206):
                                            # 若文件已存在，定位到当前末尾
                                            pos = await f.tell()
                                            # 限制写入至 chunk.end
                                            async for (
                                                data
                                            ) in response.content.iter_chunked(
                                                10240
                                            ):  # 如果数据不足10240字节则直接写入
                                                if self.end is not None:
                                                    remaining = (
                                                        self.end - self.start + 1 - pos
                                                    )
                                                    if remaining <= 0:
                                                        break
                                                    data = data[:remaining]
                                                await f.write(data)
                                                async with self.parent.lock:
                                                    self.size += len(data)
                                                    # self.parent.parent.progress.update(
                                                    #     self.parent.task,
                                                    #     advance=len(data),
                                                    # )
                                                pos += len(data)
                            except aiohttp.client_exceptions.ClientPayloadError as e:
                                await asyncio.sleep(1)
                            except Exception as e:
                                self.parent._logger.error(
                                    f"Error downloading chunk {self}: {e}"
                                )
                                await asyncio.sleep(1)
                            self.parent._logger.debug(
                                f"retrying download chunk: {self}"
                            )
                    if self.size != self.end - self.start + 1:
                        self.parent._logger.debug(
                            f"Chunk not fully downloaded, splitting chunk: {self}"
                        )
                        async with self.parent.lock:
                            new_chunk = PDManager.FileDownloader.Chunk(
                                self.parent,
                                self.start + self.size,
                                self.end,
                                os.path.join(
                                    self.parent.pdm_tmp,
                                    f"{self.parent.filename}.{self.start}",
                                ),
                                self,
                                next=self.next,
                            )
                            self.end = self.start + self.size - 1
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

    async def start_download(self):
        self._logger.debug(self)
        downloaders = []
        with self._progress:

            async def wait(downloaders: list[asyncio.Task]):
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

            while self._urls:
                for url, download_entity in self._urls.items():
                    assert isinstance(download_entity, PDManager.FileDownloader)
                    if len(downloaders) < self.threads:
                        downloaders.append(
                            asyncio.create_task(download_entity.start_download())
                        )
                    else:
                        await wait(downloaders)
                        downloaders.append(
                            asyncio.create_task(download_entity.start_download())
                        )
                    self._logger.debug(f"Starting download for {url}")
                # await asyncio.gather(*downloaders)
                while downloaders:
                    await wait(downloaders)

    def urls(self) -> List[str]:
        return list(self._urls.keys())

    def __str__(self):
        return f"PDManager(threads={self.threads}, timeout={self.timeout}, retry={self.retry}, debug={self.debug}, split={self.split}, continue_download={self.continue_download}, input_file={self.input_file}, max_concurrent_downloads={self.max_concurrent_downloads}, min_split_size={self.min_split_size})"


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
        default=sys.stdout,
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
        "-s",
        "--split",
        type=int,
        default=5,
        help="Download a file using N connections. If more than N URIs are given, first N URIs are used and remaining URLs are used for backup. If less than N URIs are given, those URLs are used more than once so that N connections total are made simultaneously. See also the --min-split-size option.",
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
        default=None,
        help="Downloads URIs found in FILE. You can specify multiple URIs for a single entity: separate URIs on a single line using the TAB character. Reads input from stdin when '-' is specified. Additionally, options can be specified after each line of URI. This optional line must start with one or more white spaces and have one option per single line. See INPUT FILE section of man page for details. See also --deferred-input option.",
    )
    parser.add_argument(
        "-x",
        "--max-concurrent-downloads",
        type=int,
        default=5,
        help="Set maximum number of parallel downloads for each URL or task.",
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
        default="20M",
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
        "urls",
        type=str,
        nargs="*",
        default=None,
        help="The URL to download.",
    )  # 可以接受多个url参数

    args = parser.parse_args()
    if args.log == "-":
        args.log = sys.stdout
    # url = "https://ftp-trace.ncbi.nlm.nih.gov/sra/sdk/3.3.0/sratoolkit.3.3.0-ubuntu64.tar.gz"
    # pdm = PDManager(max_concurrent_downloads=32, continue_download=True)
    # pdm.add_url_list([url])
    # asyncio.run(pdm.start_download())
    pdm = PDManager(
        threads=args.threads,
        log_path=args.log,
        debug=args.debug,
        split=args.split,
        continue_download=args.continue_download,
        input_file=args.input_file,
        max_concurrent_downloads=args.max_concurrent_downloads,
        min_split_size=args.min_split_size,
    )
    if args.urls:
        pdm.add_urls(
            args.urls,
        )
    asyncio.run(pdm.start_download())
