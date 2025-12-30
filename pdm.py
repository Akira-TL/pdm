#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""
@文件    :pdm.py
@说明    :模拟IDM下载方式的PDM下载器，命令行脚本
@时间    :2025/12/27 11:06:03
@作者    :Akira_TL
@版本    :1.0
"""

from glob import glob
import hashlib
import json
import os, aiofiles
import shutil
import subprocess
import sys
from typing import List, Optional
import aiohttp
from loguru import logger
from multidict import CIMultiDictProxy
import requests
from rich.progress import (
    Progress,
    Console,
)
from rich.text import Text

console = Console()
from yarl import URL
import re
import asyncio

logger.remove()
logger.add(
    lambda msg: console.print(Text.from_ansi(str(msg)), end="\n"),
    level="DEBUG",
    diagnose=True,
    colorize=True,
    format="<g>{time:MM-DD HH:mm:ss}</g> [<lvl>{level}</lvl>] <c><u>{name}</u></c> | {message}",
)


class PDManager:
    def __init__(
        self,
        workers: int = 4,
        thread: int = 1,
        timeout: int = 10,
        retry: int = 3,
        log_path: str = sys.stdout,
        split: int = 5,
        file_allocation: str = "prealloc",
        #  check_integrity:bool=False,
        continue_download: bool = False,
        input_file: str = None,
        max_concurrent_downloads: int = 5,
        #  force_sequential:bool=False,
        min_split_size: str = "10M",
    ):
        self.workers = workers
        self.thread = thread
        self.timeout = timeout
        self.retry = retry
        self.log_path = log_path
        self.split = split
        self.file_allocation = file_allocation
        # self.check_integrity = check_integrity
        self.continue_download = continue_download
        self.input_file = input_file
        self.max_concurrent_downloads = max_concurrent_downloads
        # self.force_sequential = force_sequential
        self.min_split_size = self.parse_size(min_split_size)

        self.dict_lock = asyncio.Lock()
        self.urls: dict[dict] = (
            {}
        )  # url: {md5: str, filename: str,dir_path: str,log_path: str,url_header: dict,file_size: int,...}

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
            self, parent, url, filepath, filename: str = None, md5=None, pdm_tmp=None
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
                            logger.error(
                                f"Failed to fetch md5 from url: {md5}, status code: {md5_response.status}"
                            )
            elif len(md5) == 32 and re.match(r"^[a-fA-F0-9]{32}$", md5):
                return md5.lower()
            else:
                logger.error(f"Invalid md5 value: {md5}")
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
                    logger.warning(
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
                    logger.warning(
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
            max_gap = 0
            target_chunk: "PDManager.FileDownloader.Chunk" = None
            for chunk in self.chunk_root:
                gap = chunk.end - chunk.size - chunk.start + 1
                if gap > max_gap:
                    max_gap = gap
                    target_chunk = chunk
            if target_chunk is None or max_gap < 10240:
                return None
            new_start = (
                target_chunk.start
                + target_chunk.size
                + (target_chunk.next.start if target_chunk.next else target_chunk.end)
            ) // 2
            if new_start // 10240:
                new_start -= new_start % 10240
            async with self.lock:
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
                        logger.warning(
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
                logger.error("Unknown error in creating .pdm file.")

        def merge_chunks(self):
            with open(os.path.join(self.filepath, self.filename), "wb") as outfile:
                for chunk in self.chunk_root:
                    with open(chunk.chunk_path, "rb") as infile:
                        shutil.copyfileobj(infile, outfile)
            # 清理临时文件
            shutil.rmtree(self.pdm_tmp, ignore_errors=True)

        def start_download(self):
            self.md5 = asyncio.run(self.process_md5(self.md5))
            self.filename = (
                self.filename if self.filename else asyncio.run(self.get_file_name())
            )
            os.makedirs(self.filepath, exist_ok=True)
            self.file_size = self.file_size or asyncio.run(self.get_url_file_size())
            sha = hashlib.sha256(self.url.encode("utf-8")).hexdigest()[:6]
            if self.pdm_tmp is None:
                self.pdm_tmp = os.path.join(self.filepath, f".pdm.{sha}")
            else:
                self.pdm_tmp = os.path.join(self.pdm_tmp, f".pdm.{sha}")
            os.makedirs(self.pdm_tmp, exist_ok=True)
            self.creat_info()
            self.chunk_root = asyncio.run(self.rebuild_task())
            if self.chunk_root is None:
                self.chunk_root = asyncio.run(self.build_task())
            asyncio.run(self._start_download())
            self.merge_chunks()

        async def _start_download(self):
            sem = asyncio.Semaphore(self.parent.max_concurrent_downloads)
            tasks = []

            for chunk in self.chunk_root:
                if tasks.__len__() < self.parent.max_concurrent_downloads:
                    logger.debug(f"任务数量少于最大数量，新建,chunk_root未消耗完毕")
                    tasks.append(asyncio.create_task(chunk.download()))
                else:
                    # 任意一个任务完成后再添加新的任务
                    done, pending = await asyncio.wait(
                        tasks, return_when=asyncio.FIRST_COMPLETED
                    )
                    for d in done:
                        tasks.remove(d)
                    logger.debug(
                        f"任务数量达到最大数量，完成一个再新建,chunk_root未消耗完毕"
                    )
                    tasks.append(asyncio.create_task(chunk.download()))
            while True:
                if tasks.__len__() < self.parent.max_concurrent_downloads:
                    logger.debug(f"任务数量少于最大数量，新建")
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
                logger.debug(f"任务数量达到最大数量，完成一个再新建")
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

            async def download(self):
                debug_flug = 0
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
                                logger.debug(
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
                                                if debug_flug < 10:
                                                    debug_flug += 1
                                                else:
                                                    debug_flug = 0
                                                    logger.debug(
                                                        f"real size={await f.tell()}, chunk size={self.size}, target size={(self.end - self.start + 1)}"
                                                    )
                                                pos += len(data)
                            except aiohttp.client_exceptions.ClientPayloadError as e:
                                await asyncio.sleep(1)
                            except Exception as e:
                                logger.error(f"Error downloading chunk {self}: {e}")
                                await asyncio.sleep(1)
                            logger.debug(f"retrying download chunk: {self}")
                    if self.size != self.end - self.start + 1:
                        logger.debug(
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

    def add_url(
        self,
        url: str,
        md5: str = None,
        file_name: str = None,
        dir_path: str = os.getcwd(),
        log_path: str = os.getcwd(),
    ):
        self.urls[url] = {
            "md5": md5,
            "filename": file_name,
            "dir_path": dir_path,
            "log_path": log_path,
        }

    def del_url(self, url: str):
        self.urls.pop(url, None)

    async def download(self, url: str):  # 单个文件的下载逻辑
        if url not in self.urls:
            logger.warning(f"URL not found in download list, adding it: {url}")
            self.add_url(url)


if __name__ == "__main__":
    url = "https://ftp-trace.ncbi.nlm.nih.gov/sra/sdk/3.3.0/sratoolkit.3.3.0-ubuntu64.tar.gz"

    fd = PDManager.FileDownloader(
        PDManager(max_concurrent_downloads=1, continue_download=True),
        url=url,
        filepath=os.getcwd(),
        filename=None,
        md5=None,
        pdm_tmp=None,
    )

    fd.start_download()
    # with Progress(console=console) as progress:
    #     task1 = progress.add_task("[red]Downloading...", total=100)
    #     for i in range(100):
    #         progress.update(task1, advance=1)
    #         import time
    #         time.sleep(0.1)
