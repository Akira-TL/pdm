#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@文件    :pdm.py
@说明    :模拟IDM下载方式的PDM下载器，命令行脚本
@时间    :2025/12/27 11:06:03
@作者    :Akira_TL
@版本    :1.0
'''

from glob import glob
import hashlib
import json
import os,aiofiles
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
    lambda msg: console.print(Text.from_ansi(str(msg)), end='\n'),
    level="DEBUG",
    diagnose=True,
    colorize=True,
    format="<g>{time:MM-DD HH:mm:ss}</g> [<lvl>{level}</lvl>] <c><u>{name}</u></c> | {message}",
)


class PDManager:
    def __init__(self,
                 workers: int = 4,
                 thread:int=1,
                 timeout:int=10,
                 retry:int=3,
                 log_path:str=sys.stdout,
                 split:int=5,
                 file_allocation:str='prealloc',
                #  check_integrity:bool=False,
                 continue_download:bool=False,
                 input_file:str=None,
                 max_concurrent_downloads:int=5,
                #  force_sequential:bool=False,
                min_split_size:str='10M'
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
        self.urls: dict[dict] = {} # url: {md5: str, filename: str,dir_path: str,log_path: str,url_header: dict,file_size: int,...}

    def parse_size(self, size_str: str) -> int:
        size_str = str(size_str).strip().upper()
        size_map = {'K': 1024, 'M': 1024**2, 'G': 1024**3}
        size = 1
        for i in range(len(size_str)-1, -1, -1):
            unit = size_str[i]
            if unit in size_map:
                size *= size_map[unit]
                continue
            break
        num = size_str[:i+1]
        if re.match(r'^\d+(\.\d+)?$', num):
            return int(float(num) * size)
        else:
            raise ValueError(f"Invalid size format: {size_str}")

    class FileDownloader:
        def __init__(self,parent,url,filepath, filename: str=None,md5=None,pdm_tmp=None):
            self.parent:PDManager = parent
            self.url = url
            self.filepath = filepath
            self.filename = filename
            self.md5 = md5
            self.pdm_tmp = pdm_tmp
            self.file_size: int = 0
            self.chunk_root: 'PDManager.FileDownloader.Chunk' | None = None
            self.lock = asyncio.Lock()
            self.header_info = None
        
        def __str__(self):
            chunks = []
            for chunk in self.chunk_root:
                chunks.append(str(chunk))
            return f"FileDownloader(url={self.url}, filepath={self.filepath}, filename={self.filename}, md5={self.md5}, pdm_tmp={self.pdm_tmp}, file_size={self.file_size})\n{"\n".join(chunks)}"
        
        async def process_md5(self,md5): # 处理传入的md5值
            if self.md5 is None:
                return None
            elif os.path.exists(self.filepath):
                async with aiofiles.open(self.filepath, 'r') as f:
                    md5 = await f.read()
                    return md5.strip()
            elif re.match(r'^(http|https|ftp)://', self.md5):
                async with aiohttp.ClientSession() as session:
                    async with session.get(self.md5) as md5_response:
                        if md5_response.status == 200:
                            md5_value = await md5_response.text()
                            return md5_value.strip()
                        else:
                            logger.error(f"Failed to fetch md5 from url: {self.md5}, status code: {md5_response.status}")
            elif self.md5.__len__() == 32 and re.match(r'^[a-fA-F0-9]{32}$', self.md5):
                return md5.lower()
            else:
                logger.error(f"Invalid md5 value: {self.md5}")
                return None
        
        async def get_file_name(self) -> str:
            async with aiohttp.ClientSession() as session:
                if self.header_info is None:
                    async with session.head(self.url, allow_redirects=True) as response:
                        self.header_info = response.headers
                cd = self.header_info.get('Content-Disposition')
                if cd:
                    fname = re.findall('filename="(.+)"', cd)
                    if fname:
                        return fname[0]
                fname = os.path.basename(URL(response.url).path)
                if fname == '':
                    fname = f"{hashlib.sha256(self.url.encode('utf-8')).hexdigest()[:6]}.dat"
                    logger.warning(f"Cannot get filename from URL, use hash url as filename: {fname}")
                return fname

        async def get_url_file_size(self) -> int:
            async with aiohttp.ClientSession() as session:
                if self.header_info is not None:
                    async with session.head(self.url, allow_redirects=True) as response:
                        self.header_info = response.headers
                file_size = self.header_info.get('Content-Length')
                if file_size:
                    return int(file_size)
                else:
                    return -1 # -1标记未知大小，与None区分

        async def build_task(self):
            """
                新建分块链表
                Returns:
                    PDManager.FileDownloader.ChunkManager: 分块链表头
            """
            chunk_size = self.file_size // self.parent.split # TODO file_size为-1时的处理
            if chunk_size < self.parent.min_split_size:
                chunk_size = self.parent.min_split_size
            elif chunk_size // 10240:
                    chunk_size -= chunk_size % 10240 # 保证chunk_size是10K的整数倍
            starts = list(range(0, self.file_size, chunk_size))
            if starts[-1] < 102400:
                starts[-2] += starts[-1]
                starts.pop()
            root = None
            for i in range(len(starts)):
                start = starts[i]
                end = starts[i+1] - 1 if i + 1 < len(starts) else self.file_size - 1
                if root is None:
                    root = cur = PDManager.FileDownloader.Chunk(self,start,end, os.path.join(self.pdm_tmp,f"{self.filename}.{start}"))
                    continue
                if start >= self.file_size: # TODO 没有问题就移除
                    logger.warning(f"start {start} >= file_size {self.file_size}, break")
                    break
                cur.next = PDManager.FileDownloader.Chunk(self,start,end, os.path.join(self.pdm_tmp,f"{self.filename}.{start}"))
                cur = cur.next
            assert root is not None
            return root
        
        async def rebuild_task(self):
            """
                根据已有的分块文件重建分块链表
                Returns:
                    PDManager.FileDownloader.ChunkManager: 分块链表头
            """
            file_list = {p.removeprefix(os.path.join(self.pdm_tmp,self.filename)+'.'):p for p in glob(os.path.join(self.pdm_tmp,self.filename)+'*')}
            ordered_starts = sorted([int(k) for k in file_list.keys()])
            root = None
            if not ordered_starts:
                return root # None
            for i in range(len(ordered_starts)):
                start = ordered_starts[i]
                end = ordered_starts[i+1] - 1 if i + 1 < len(ordered_starts) else self.file_size - 1
                if root is None:
                    root = cur = PDManager.FileDownloader.Chunk(self,start,end,file_list[str(start)])
                    continue
                cur.next = PDManager.FileDownloader.Chunk(self,start,end,file_list[str(start)])
                cur = cur.next
            return root
        
        async def create_chunk(self) -> 'PDManager.FileDownloader.Chunk' | None:
            # 遍历chunk列表，先找到间隔最大的正在下载的chunk，然后在其间隙中创建新的chunk，间隔小于102400则返回None
            max_gap = 0
            target_chunk: 'PDManager.FileDownloader.Chunk' | None = None
            for chunk in self.chunk_root:
                gap = chunk.end - chunk.size - chunk.start + 1
                if gap > max_gap:
                    max_gap = gap
                    target_chunk = chunk
            if target_chunk is None or max_gap < 10240:
                return None
            new_start = (target_chunk.start + target_chunk.size + (target_chunk.next.start if target_chunk.next else target_chunk.end)) // 2
            if new_start // 10240:
                new_start -= new_start % 10240
            async with self.lock:
                new_chunk = PDManager.FileDownloader.Chunk(self,new_start,target_chunk.next.start - 1 if target_chunk.next else target_chunk.end,os.path.join(self.pdm_tmp,f"{self.filename}.{new_start}"),target_chunk,next=target_chunk.next)
                if target_chunk.next is None:
                    new_chunk.end = target_chunk.end
                else:
                    new_chunk.end = target_chunk.next.start - 1
                target_chunk.end = new_start - 1
                target_chunk.next = new_chunk
            return new_chunk

        def creat_info(self):
            if not self.parent.continue_download or not os.path.exists(os.path.join(self.pdm_tmp,".pdm")):
                shutil.rmtree(self.pdm_tmp,ignore_errors=True)
                os.makedirs(self.pdm_tmp, exist_ok=True)
                with open(os.path.join(self.pdm_tmp,".pdm"), 'w') as f:
                    info = {
                        'url': self.url,
                        'filename': self.filename,
                        'md5': self.md5,
                        'file_size': self.file_size,
                    }
                    json.dump(info, f, indent=4)
            elif os.path.exists(os.path.join(self.pdm_tmp,".pdm")):
                with open(os.path.join(self.pdm_tmp,".pdm"), 'r') as f:
                    info = json.load(f)
                    if info.get('md5') != self.md5 or info.get('file_size') != self.file_size or info.get('filename') != self.filename or info.get('url') != self.url:
                        logger.warning("Existing .pdm file info does not match current download info, recreating .pdm file.")
                        shutil.rmtree(self.pdm_tmp)
                        os.makedirs(self.pdm_tmp, exist_ok=True)
                        with open(os.path.join(self.pdm_tmp,".pdm"), 'w') as f:
                            info = {
                                'url': self.url,
                                'filename': self.filename,
                                'md5': self.md5,
                                'file_size': self.file_size,
                            }
                            json.dump(info, f, indent=4)
            else:
                logger.error("Unknown error in creating .pdm file.")

        def merge_chunks(self):
            with open(os.path.join(self.filepath,self.filename), 'wb') as outfile:
                for chunk in self.chunk_root:
                    with open(chunk.chunk_path, 'rb') as infile:
                        shutil.copyfileobj(infile, outfile)
            # 清理临时文件
            # shutil.rmtree(self.pdm_tmp,ignore_errors=True)
        
        def start_download(self):
            self.md5 = asyncio.run(self.process_md5(self.md5))
            self.filename = self.filename if self.filename else asyncio.run(self.get_file_name())
            os.makedirs(self.filepath, exist_ok=True)
            self.file_size = self.file_size or asyncio.run(self.get_url_file_size())
            sha = hashlib.sha256(self.url.encode('utf-8')).hexdigest()[:6]
            if self.pdm_tmp is None:
                self.pdm_tmp = os.path.join(self.filepath,f".pdm.{sha}")
            else:
                self.pdm_tmp = os.path.join(self.pdm_tmp , f".pdm.{sha}")
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
                    logger.info(f"任务数量少于最大数量，新建,chunk_root未消耗完毕")
                    tasks.append(asyncio.create_task(chunk.download()))
                else:
                    # 任意一个任务完成后再添加新的任务
                    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    for d in done:
                        tasks.remove(d)
                    logger.info(f"任务数量达到最大数量，完成一个再新建,chunk_root未消耗完毕")
                    tasks.append(asyncio.create_task(chunk.download()))
            while True:
                if tasks.__len__() < self.parent.max_concurrent_downloads:
                    logger.info(f"任务数量少于最大数量，新建")
                    new_chunk = await self.create_chunk() # TODO
                    if new_chunk is None:
                        break
                    tasks.append(asyncio.create_task(new_chunk.download()))
                    continue
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for d in done:
                    tasks.remove(d)
                logger.info(f"任务数量达到最大数量，完成一个再新建")
                new_chunk = await self.create_chunk() # TODO
                if new_chunk is None:
                    break
                tasks.append(asyncio.create_task(new_chunk.download()))
            await asyncio.gather(*tasks)

        class Chunk:
            def __init__(self,parent:PDManager.FileDownloader,start: int, end: int, chunk_path:str,forward:'PDManager.FileDownloader.Chunk'=None,next: 'PDManager.FileDownloader.Chunk'=None):
                self.parent = parent
                self.start = start
                self.end = end
                self.chunk_path = chunk_path
                if os.path.exists(chunk_path):
                    self.size = os.path.getsize(chunk_path)
                else:
                    self.size = 0
                self.forward: 'PDManager.FileDownloader.Chunk' = forward
                self.next: 'PDManager.FileDownloader.Chunk' = next

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
                    file_mode = 'ab' if os.path.exists(self.chunk_path) else 'wb'
                    async with aiofiles.open(self.chunk_path, file_mode) as f:
                        # 断连续传
                        for _ in range(self.parent.parent.retry):
                            # 已完整下载
                            if os.path.exists(self.chunk_path):
                                if self.size == self.end - self.start + 1:
                                    return self
                            try:
                                # 构造 Range
                                headers['Range'] = f'bytes={self.start + self.size}-{self.end}'
                                logger.debug(f"Downloading chunk: {self}, with headers: {headers}")
                                # 仅在未完成时下载
                                if self.size < self.end - self.start + 1:
                                    async with session.get(self.parent.url, headers=headers) as response:
                                        if response.status in (200, 206):
                                            # 若文件已存在，定位到当前末尾
                                            pos = await f.tell()
                                            # 限制写入至 chunk.end
                                            async for data in response.content.iter_chunked(10240): # 如果数据不足10240字节则直接写入
                                                if self.end is not None:
                                                    remaining = self.end - self.start + 1 - pos
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
                                                    logger.debug(f"real size={await f.tell()}, chunk size={self.size}, target size={(self.end - self.start + 1)}")
                                                pos += len(data)
                            except aiohttp.client_exceptions.ClientPayloadError as e:
                                await asyncio.sleep(1)
                            except Exception as e:
                                logger.error(f"Error downloading chunk {self}: {e}")
                                await asyncio.sleep(1)
                            logger.debug(f"retrying download chunk: {self}")
                    if self.size != self.end - self.start + 1:
                        logger.debug(f"Chunk not fully downloaded, splitting chunk: {self}")
                        async with self.parent.lock: # 
                            new_chunk = PDManager.FileDownloader.Chunk(self.parent,
                                                                       self.start + self.size,
                                                                       self.end,
                                                                       os.path.join(self.parent.pdm_tmp,f"{self.parent.filename}.{self.start}"),
                                                                       self,next=self.next)
                            self.end = self.start + self.size - 1
                            self.next = new_chunk
                    return self


    # def rebuild_chunk(self, path: str,fname:str,fsize:int) -> 'PDManager.FileDownloader.ChunkManager':
    #     """
    #         根据已有的分块文件重建分块链表
    #         Args:
    #             path (str): 分块文件所在目录
    #             fname (str): 原始文件名
    #             fsize (int): 原始文件大小
    #         Returns:
    #             PDManager.DownloadFileInfo.ChunkManager: 分块链表头
    #     """
    #     file_list = {p.removeprefix(os.path.join(path,fname)+'.'):p for p in glob(os.path.join(path,fname)+'*')}
    #     ordered_starts = sorted([int(k) for k in file_list.keys()])
    #     root = None
    #     if not ordered_starts:
    #         return None
    #     for start in ordered_starts:
    #         if root is None:
    #             root = cur = PDManager.FileDownloader.Chunk(start,file_list[str(start)])
    #             continue
    #         cur.next = PDManager.FileDownloader.Chunk(start,file_list[str(start)])
    #         cur.end = start - 1
    #         cur = cur.next
    #     cur.end = fsize - 1
    #     return root

    # async def get_file_name(self, url: str) -> str:
    #     if self.urls.get(url):
    #         if self.urls[url].get('filename'):
    #             return self.urls[url]['filename']
    #     async with aiohttp.ClientSession() as session:
    #         async with session.head(url, allow_redirects=True) as response:
    #             cd = response.headers.get('Content-Disposition')
    #             if cd:
    #                 fname = re.findall('filename="(.+)"', cd)
    #                 if fname:
    #                     return fname[0]
    #             fname = os.path.basename(URL(response.url).path)
    #             if fname == '':
    #                 fname = f"{hashlib.sha256(url.encode('utf-8')).hexdigest()}.dat"
    #                 logger.warning(f"Cannot get filename from URL, use hash url as filename: {fname}")
    #             return fname
    
    # async def set_file_info(self,url:str):
    #     async with aiohttp.ClientSession() as session:
    #         async with session.head(url, allow_redirects=True) as response:
    #             if not self.urls.get(url).get('filename'):
    #                 cd = response.headers.get('Content-Disposition')
    #                 if cd:
    #                     fname = re.findall('filename="(.+)"', cd)
    #                     if fname:
    #                         self.urls[url]['filename'] = fname[0]
    #                         return
    #                 fname = os.path.basename(URL(response.url).path)
    #                 if fname == '':
    #                     fname = f"pdm.{hashlib.sha256(url.encode('utf-8')).hexdigest()[:6]}.dat"
    #                     logger.warning(f"Cannot get filename from URL, use hash url as filename: {fname}")
    #                 self.urls[url]['filename'] = fname
    #             if self.urls.get(url).get('md5'):
    #                 # 判断md5是url还是值
    #                 md5 = self.urls[url]['md5']
    #                 if re.match(r'^(http|https|ftp)://', md5):
    #                     async with session.get(md5) as md5_response:
    #                         if md5_response.status == 200:
    #                             md5_value = await md5_response.text()
    #                             self.urls[url]['md5'] = md5_value.strip()
    #                         else:
    #                             logger.error(f"Failed to fetch md5 from url: {md5}, status code: {md5_response.status}")
    #             if not self.urls.get(url).get('file_size'):
    #                 file_size = response.headers.get('Content-Length')
    #                 if file_size:
    #                     self.urls[url]['file_size'] = int(file_size)
    #                 else:
    #                     self.urls[url]['file_size'] = None
                

    def add_url(self, url: str,md5: str = None, file_name: str = None,dir_path: str = os.getcwd(),log_path: str = os.getcwd()):
        self.urls[url] = {'md5': md5, 'filename': file_name, 'dir_path': dir_path, 'log_path': log_path}

    def del_url(self, url: str):
        self.urls.pop(url, None)

    # def create_new_download_chunk_list(self, url: str, pdm_tmp: str):
    #     file_size = self.urls[url]['file_size']
    #     if file_size is None:
    #         return PDManager.FileDownloader.Chunk(0,os.path.join(pdm_tmp,f"{self.urls[url]['filename']}.0"))
    #     chunk_size = file_size // self.split
    #     if chunk_size < self.min_split_size:
    #         chunk_size = self.min_split_size
    #     if chunk_size // 1024:
    #         chunk_size -= chunk_size % 1024 # 保证chunk_size是1K的整数倍
    #     starts = list(range(0, file_size, chunk_size))
    #     root = None
    #     for start in starts:
    #         if root is None:
    #             root = cur = PDManager.FileDownloader.Chunk(start,os.path.join(pdm_tmp,f"{self.urls[url]['filename']}.{start}"))
    #             continue
    #         if start >= file_size:
    #             break
    #         cur.next = PDManager.FileDownloader.Chunk(start,os.path.join(pdm_tmp,f"{self.urls[url]['filename']}.{start}"))
    #         cur.end = start - 1
    #         cur = cur.next
    #     cur.end = file_size - 1
    #     return root

    # async def download_chunk(self, url: str, pdm_tmp: str, chunk: 'PDManager.FileDownloader.Chunk', sem: asyncio.Semaphore):
    #     debug_flug = 0
    #     assert chunk.end is not None or chunk.size >= 0
    #     async with sem:
    #         headers = {}
    #         # 复用会话
    #         async with aiohttp.ClientSession() as session:
    #             # 仅打开一次文件（追加或新建）
    #             file_mode = 'ab' if os.path.exists(chunk.fpath) else 'wb'
    #             async with aiofiles.open(chunk.fpath, file_mode) as f:
    #                 # 断连续传
    #                 for _ in range(self.retry):
    #                     try:
    #                         # 已完整下载
    #                         if os.path.exists(chunk.fpath):
    #                             on_disk = os.path.getsize(chunk.fpath)
    #                             if chunk.size == on_disk and chunk.size >= chunk.end - chunk.start + 1:
    #                                 return chunk

    #                         # 构造 Range
    #                         headers['Range'] = f'bytes={chunk.start + chunk.size}-{chunk.end}'

    #                         # 仅在未完成时下载
    #                         if chunk.size < chunk.end - chunk.start + 1:
    #                             async with session.get(url, headers=headers) as response:
    #                                 if response.status in (200, 206):
    #                                     # 若文件已存在，定位到当前末尾
    #                                     pos = await f.tell()
    #                                     # 限制写入至 chunk.end
    #                                     async for data in response.content.iter_chunked(10240): # 如果数据不足10240字节则直接写入
    #                                         if chunk.end is not None:
    #                                             remaining = chunk.end - chunk.start + 1 - pos
    #                                             if remaining <= 0:
    #                                                 break
    #                                             data = data[:remaining]
    #                                         await f.write(data)
    #                                         async with self.dict_lock,chunk.lock:
    #                                             chunk.size += len(data)
    #                                         if debug_flug < 10:
    #                                             debug_flug += 1
    #                                         else:
    #                                             debug_flug = 0
    #                                             logger.debug(f"real size={await f.tell()}, chunk size={chunk.size}, pos={pos}, wrote={len(data)} bytes")
    #                                         pos += len(data)
    #                     except aiohttp.client_exceptions.ClientPayloadError as e:
    #                         await asyncio.sleep(1)
    #             if chunk.size != chunk.end - chunk.start + 1:
    #                 async with self.dict_lock,PDManager.FileDownloader.Chunk.lock:
    #                     new_chunk = PDManager.FileDownloader.Chunk(chunk.start + chunk.size,chunk.fpath.split(),chunk.next)
    #                     new_chunk.end = chunk.end
    #                     chunk.end = chunk.start + chunk.size - 1
    #                     chunk.next = new_chunk

    #     logger.debug(f"chunk info: {chunk} ,next: {chunk.next if chunk.next else 'None'}")
    #     return chunk

    # async def create_a_new_download_chunk(self, chunk_root: 'PDManager.FileDownloader.Chunk') -> 'PDManager.FileDownloader.Chunk' | None:
    #     # 遍历chunk列表，先找到间隔最大的正在下载的chunk，然后在其间隙中创建新的chunk，间隔小于102400则返回None
    #     max_gap = 0
    #     target_chunk: 'PDManager.FileDownloader.Chunk' | None = None
    #     for chunk in chunk_root:
    #         gap = chunk.end - chunk.size - chunk.start + 1
    #         if gap > max_gap:
    #             max_gap = gap
    #             target_chunk = chunk
    #     if target_chunk is None or max_gap < 102400:
    #         return None
    #     new_start = (target_chunk.start + target_chunk.size + (target_chunk.next.start if target_chunk.next else target_chunk.end)) // 2
    #     if new_start // 1024:
    #         new_start -= new_start % 1024
    #     async with self.dict_lock,PDManager.FileDownloader.Chunk.lock:
    #         new_chunk = PDManager.FileDownloader.Chunk(new_start,os.path.join(os.path.dirname(target_chunk.fpath),f"{self.urls[url]['filename']}.{new_start}"),target_chunk.next if target_chunk.next else None)
    #         if target_chunk.next is None:
    #             new_chunk.end = target_chunk.end
    #         else:
    #             new_chunk.end = target_chunk.next.start - 1
    #         target_chunk.end = new_start - 1
    #         target_chunk.next = new_chunk
    #     return new_chunk

    # async def start_download(self, url: str, pdm_tmp: str, chunk_root: 'PDManager.FileDownloader.Chunk' | None):
    #     if chunk_root is None:
    #         logger.info("starting fresh download...")
    #         chunk_root = self.create_new_download_chunk_list(url, pdm_tmp)
    #     sem = asyncio.Semaphore(self.max_concurrent_downloads)
    #     tasks = []

    #     for chunk in chunk_root:
    #         if tasks.__len__() < self.max_concurrent_downloads:
    #             tasks.append(asyncio.create_task(self.download_chunk(url, pdm_tmp, chunk, sem)))
    #         else:
    #             # 任意一个任务完成后再添加新的任务
    #             done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    #             for d in done:
    #                 tasks.remove(d)
    #             tasks.append(asyncio.create_task(self.download_chunk(url, pdm_tmp, chunk, sem)))
    #     while True:
    #         if tasks.__len__() < self.max_concurrent_downloads:
    #             logger.info(f"任务数量少于最大数量，新建")
    #             new_chunk = await self.create_a_new_download_chunk(chunk_root) # TODO
    #             if new_chunk is None:
    #                 break
    #             tasks.append(asyncio.create_task(self.download_chunk(url, pdm_tmp, chunk, sem)))
    #             continue
    #         done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    #         for d in done:
    #             tasks.remove(d)
    #         logger.info(f"任务数量达到最大数量，完成一个再新建")
    #         new_chunk = await self.create_a_new_download_chunk(chunk_root) # TODO
    #         if new_chunk is None:
    #             break
    #         tasks.append(asyncio.create_task(self.download_chunk(url, pdm_tmp, new_chunk, sem)))
    #     await asyncio.gather(*tasks)

    async def download(self, url: str): # 单个文件的下载逻辑
        if url not in self.urls:
            logger.warning(f"URL not found in download list, adding it: {url}")
            self.add_url(url)
        # await self.set_file_info(url)
        # os.makedirs(self.urls[url]['dir_path'], exist_ok=True)
        # pdm_tmp = os.path.join(self.urls[url]['dir_path'],f"pdm.{hashlib.sha256(url.encode('utf-8')).hexdigest()[:6]}")
        # os.makedirs(pdm_tmp, exist_ok=True)
        
        # chunk_root = self.rebuild_chunk(pdm_tmp,self.urls[url]['filename'],self.urls[url]['file_size'])
        # await self.start_download(url,pdm_tmp,chunk_root)
        
        


if __name__ == '__main__':
    # pdm = PDManager(max_concurrent_downloads=5)
    url = "https://ftp-trace.ncbi.nlm.nih.gov/sra/sdk/3.3.0/sratoolkit.3.3.0-ubuntu64.tar.gz"

    fd = PDManager.FileDownloader(PDManager(max_concurrent_downloads=1,continue_download = True),
                                    url=url,
                                    filepath=os.getcwd(),
                                    filename=None,
                                    md5=None,
                                    pdm_tmp=None)

    fd.start_download()
    # with Progress(console=console) as progress:
    #     task1 = progress.add_task("[red]Downloading...", total=100)
    #     for i in range(100):
    #         progress.update(task1, advance=1)
    #         import time
    #         time.sleep(0.1)