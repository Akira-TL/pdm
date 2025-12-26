#!/usr/bin/env python3
"""
多线程断点续传下载管理器
Multi-threaded Download Manager with Resume Capability

模拟IDM（互联网下载管理器）的多线程断点续传下载流程
Simulates IDM (Internet Download Manager) multi-threaded resume download process
"""

import os
import sys
import json
import time
import threading
import requests
from urllib.parse import urlparse, unquote
from typing import Dict, List, Optional
import argparse
import logging


class DownloadConfig:
    """下载配置类 - Download Configuration"""
    
    def __init__(self, 
                 num_threads: int = 32,
                 speed_threshold: int = 10240,  # 10KB/s
                 chunk_size: int = 8192,
                 timeout: int = 30,
                 max_retries: int = 3):
        """
        初始化下载配置
        
        Args:
            num_threads: 线程数量 (默认32)
            speed_threshold: 速度阈值，低于此速度将重启线程 (默认10KB/s)
            chunk_size: 每次读取的块大小 (默认8KB)
            timeout: 请求超时时间 (默认30秒)
            max_retries: 最大重试次数 (默认3次)
        """
        self.num_threads = num_threads
        self.speed_threshold = speed_threshold
        self.chunk_size = chunk_size
        self.timeout = timeout
        self.max_retries = max_retries


class DownloadThread:
    """下载线程类 - Download Thread"""
    
    def __init__(self, thread_id: int, start: int, end: int, url: str, 
                 output_file: str, config: DownloadConfig, headers: Dict[str, str]):
        """
        初始化下载线程
        
        Args:
            thread_id: 线程ID
            start: 起始字节位置
            end: 结束字节位置
            url: 下载链接
            output_file: 输出文件路径
            config: 下载配置
            headers: HTTP请求头
        """
        self.thread_id = thread_id
        self.start = start
        self.end = end
        self.current_pos = start
        self.url = url
        self.output_file = output_file
        self.config = config
        self.headers = headers.copy()
        self.completed = False
        self.running = False
        self.speed = 0.0  # 当前下载速度 (bytes/s)
        self.downloaded = 0  # 已下载字节数
        self.last_check_time = time.time()
        self.last_check_bytes = 0
        self.thread = None
        self.lock = threading.Lock()
        
    def get_chunk_file(self) -> str:
        """获取线程对应的临时文件路径"""
        return f"{self.output_file}.part{self.thread_id}"
    
    def calculate_speed(self):
        """计算当前下载速度"""
        current_time = time.time()
        time_diff = current_time - self.last_check_time
        
        if time_diff >= 1.0:  # 每秒计算一次速度
            bytes_diff = self.downloaded - self.last_check_bytes
            self.speed = bytes_diff / time_diff
            self.last_check_time = current_time
            self.last_check_bytes = self.downloaded
    
    def download(self):
        """执行下载任务"""
        self.running = True
        retry_count = 0
        
        while retry_count < self.config.max_retries and not self.completed:
            try:
                # 设置Range请求头（仅当有范围时）
                range_header = self.headers.copy()
                if self.end > 0:  # 支持Range的多线程下载
                    range_header['Range'] = f'bytes={self.current_pos}-{self.end}'
                
                # 发送HTTP请求
                response = requests.get(
                    self.url, 
                    headers=range_header, 
                    stream=True,
                    timeout=self.config.timeout
                )
                
                if response.status_code not in [200, 206]:
                    logging.warning(f"线程 {self.thread_id}: HTTP状态码 {response.status_code}")
                    retry_count += 1
                    time.sleep(1)
                    continue
                
                # 打开临时文件并追加写入
                chunk_file = self.get_chunk_file()
                mode = 'ab' if os.path.exists(chunk_file) else 'wb'
                
                with open(chunk_file, mode) as f:
                    for chunk in response.iter_content(chunk_size=self.config.chunk_size):
                        if chunk:
                            f.write(chunk)
                            chunk_len = len(chunk)
                            
                            with self.lock:
                                self.current_pos += chunk_len
                                self.downloaded += chunk_len
                                self.calculate_speed()
                            
                            # 检查是否完成（仅当有范围限制时）
                            if self.end > 0 and self.current_pos > self.end:
                                break
                
                # 下载完成
                # 对于单线程下载（end=0），当response完成时表示下载完成
                # 对于多线程下载（end>0），当到达结束位置时表示下载完成
                if self.end == 0 or self.current_pos >= self.end:
                    self.completed = True
                    logging.info(f"线程 {self.thread_id}: 下载完成")
                    break
                    
            except Exception as e:
                logging.error(f"线程 {self.thread_id}: 下载错误 - {str(e)}")
                retry_count += 1
                time.sleep(2)
        
        self.running = False
        
        if not self.completed:
            logging.error(f"线程 {self.thread_id}: 下载失败，已达到最大重试次数")
    
    def start(self):
        """启动线程"""
        if not self.running and not self.completed:
            self.thread = threading.Thread(target=self.download)
            self.thread.daemon = True
            self.thread.start()
    
    def restart(self):
        """重启线程"""
        if self.running:
            logging.info(f"线程 {self.thread_id}: 速度过慢 ({self.speed/1024:.2f} KB/s)，重启线程")
            self.running = False
            if self.thread:
                self.thread.join(timeout=5)
        self.start()
    
    def get_state(self) -> Dict:
        """获取线程状态"""
        with self.lock:
            return {
                'thread_id': self.thread_id,
                'start': self.start,
                'end': self.end,
                'current_pos': self.current_pos,
                'completed': self.completed,
                'downloaded': self.downloaded,
                'speed': self.speed
            }
    
    def load_state(self, state: Dict):
        """加载线程状态"""
        with self.lock:
            self.current_pos = state.get('current_pos', self.start)
            self.completed = state.get('completed', False)
            self.downloaded = state.get('downloaded', 0)


class DownloadManager:
    """下载管理器类 - Download Manager"""
    
    def __init__(self, url: str, output_path: Optional[str] = None, 
                 config: Optional[DownloadConfig] = None):
        """
        初始化下载管理器
        
        Args:
            url: 下载链接
            output_path: 输出文件路径（可选）
            config: 下载配置（可选）
        """
        self.url = url
        self.config = config or DownloadConfig()
        self.output_path = output_path or self._get_filename_from_url()
        self.state_file = f"{self.output_path}.state"
        self.threads: List[DownloadThread] = []
        self.total_size = 0
        self.headers = self._get_default_headers()
        self.monitor_thread = None
        self.running = False
        
        # 设置日志
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('download.log', encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
    
    def _get_default_headers(self) -> Dict[str, str]:
        """获取默认HTTP请求头"""
        return {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': '*/*',
            'Accept-Encoding': 'identity',
            'Connection': 'keep-alive'
        }
    
    def _get_filename_from_url(self) -> str:
        """从URL中提取文件名"""
        parsed = urlparse(self.url)
        filename = os.path.basename(parsed.path)
        if filename:
            return unquote(filename)
        return 'downloaded_file'
    
    def _get_file_size(self) -> int:
        """获取文件大小"""
        try:
            response = requests.head(self.url, headers=self.headers, timeout=self.config.timeout)
            
            # 检查是否支持Range请求
            if 'Accept-Ranges' not in response.headers or response.headers['Accept-Ranges'] == 'none':
                logging.warning("服务器不支持Range请求，将使用单线程下载")
                self.config.num_threads = 1
            
            # 获取文件大小
            content_length = response.headers.get('Content-Length')
            if content_length:
                return int(content_length)
            else:
                logging.warning("无法获取文件大小，将使用单线程下载")
                self.config.num_threads = 1
                return 0
                
        except Exception as e:
            logging.error(f"获取文件信息失败: {str(e)}")
            raise
    
    def _initialize_threads(self):
        """初始化下载线程"""
        if self.total_size == 0:
            # 单线程下载
            thread = DownloadThread(0, 0, 0, self.url, self.output_path, 
                                   self.config, self.headers)
            self.threads.append(thread)
        else:
            # 多线程下载
            chunk_size = self.total_size // self.config.num_threads
            
            for i in range(self.config.num_threads):
                start = i * chunk_size
                end = start + chunk_size - 1 if i < self.config.num_threads - 1 else self.total_size - 1
                
                thread = DownloadThread(i, start, end, self.url, self.output_path,
                                       self.config, self.headers)
                self.threads.append(thread)
        
        logging.info(f"初始化 {len(self.threads)} 个下载线程")
    
    def _save_state(self):
        """保存下载状态到文件"""
        state = {
            'url': self.url,
            'output_path': self.output_path,
            'total_size': self.total_size,
            'num_threads': self.config.num_threads,
            'threads': [thread.get_state() for thread in self.threads]
        }
        
        try:
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            logging.error(f"保存状态失败: {str(e)}")
    
    def _load_state(self) -> bool:
        """从文件加载下载状态"""
        if not os.path.exists(self.state_file):
            return False
        
        try:
            with open(self.state_file, 'r', encoding='utf-8') as f:
                state = json.load(f)
            
            # 验证URL是否匹配
            if state.get('url') != self.url:
                logging.warning("状态文件URL不匹配，将重新开始下载")
                return False
            
            self.total_size = state.get('total_size', 0)
            
            # 恢复线程状态
            thread_states = state.get('threads', [])
            for thread_state in thread_states:
                thread = DownloadThread(
                    thread_state['thread_id'],
                    thread_state['start'],
                    thread_state['end'],
                    self.url,
                    self.output_path,
                    self.config,
                    self.headers
                )
                thread.load_state(thread_state)
                self.threads.append(thread)
            
            logging.info(f"从状态文件恢复下载，已完成 {self._get_downloaded_size()} / {self.total_size} 字节")
            return True
            
        except Exception as e:
            logging.error(f"加载状态失败: {str(e)}")
            return False
    
    def _get_downloaded_size(self) -> int:
        """获取已下载的总字节数"""
        return sum(thread.downloaded for thread in self.threads)
    
    def _get_total_speed(self) -> float:
        """获取总下载速度"""
        return sum(thread.speed for thread in self.threads)
    
    def _monitor_threads(self):
        """监控线程状态和下载速度"""
        while self.running:
            time.sleep(5)  # 每5秒检查一次
            
            for thread in self.threads:
                if not thread.completed and thread.running:
                    # 检查速度是否低于阈值
                    if thread.speed < self.config.speed_threshold and thread.downloaded > 0:
                        thread.restart()
            
            # 保存状态
            self._save_state()
            
            # 显示进度
            self._display_progress()
    
    def _display_progress(self):
        """显示下载进度"""
        downloaded = self._get_downloaded_size()
        total_speed = self._get_total_speed()
        
        if self.total_size > 0:
            progress = (downloaded / self.total_size) * 100
            remaining = self.total_size - downloaded
            eta = remaining / total_speed if total_speed > 0 else 0
            
            logging.info(
                f"进度: {progress:.2f}% ({downloaded}/{self.total_size} 字节) | "
                f"速度: {total_speed/1024:.2f} KB/s | "
                f"预计剩余时间: {eta:.0f}秒"
            )
        else:
            logging.info(
                f"已下载: {downloaded} 字节 | "
                f"速度: {total_speed/1024:.2f} KB/s"
            )
    
    def _merge_chunks(self):
        """合并所有下载的数据块"""
        logging.info("开始合并文件块...")
        
        try:
            with open(self.output_path, 'wb') as output:
                for thread in sorted(self.threads, key=lambda t: t.thread_id):
                    chunk_file = thread.get_chunk_file()
                    if os.path.exists(chunk_file):
                        with open(chunk_file, 'rb') as chunk:
                            output.write(chunk.read())
                        # 删除临时文件
                        os.remove(chunk_file)
            
            # 删除状态文件
            if os.path.exists(self.state_file):
                os.remove(self.state_file)
            
            logging.info(f"文件合并完成: {self.output_path}")
            
        except Exception as e:
            logging.error(f"合并文件失败: {str(e)}")
            raise
    
    def start(self):
        """开始下载"""
        logging.info(f"开始下载: {self.url}")
        logging.info(f"输出文件: {self.output_path}")
        logging.info(f"线程数: {self.config.num_threads}")
        
        # 尝试加载之前的状态
        if not self._load_state():
            # 获取文件大小
            self.total_size = self._get_file_size()
            logging.info(f"文件大小: {self.total_size} 字节")
            
            # 初始化线程
            self._initialize_threads()
        
        # 启动所有线程
        self.running = True
        for thread in self.threads:
            if not thread.completed:
                thread.start()
        
        # 启动监控线程
        self.monitor_thread = threading.Thread(target=self._monitor_threads)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        
        # 等待所有线程完成
        try:
            while True:
                all_completed = all(thread.completed for thread in self.threads)
                if all_completed:
                    break
                time.sleep(1)
                
                # 检查是否有线程需要重启
                for thread in self.threads:
                    if not thread.completed and not thread.running:
                        thread.start()
        
        except KeyboardInterrupt:
            logging.info("下载被用户中断")
            self.running = False
            self._save_state()
            sys.exit(0)
        
        self.running = False
        
        # 合并文件
        self._merge_chunks()
        
        logging.info("下载完成！")
    
    def stop(self):
        """停止下载"""
        self.running = False
        self._save_state()


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='多线程断点续传下载管理器 - Multi-threaded Download Manager'
    )
    parser.add_argument('url', nargs='?', help='下载链接 (Download URL)')
    parser.add_argument('-o', '--output', help='输出文件路径 (Output file path)')
    parser.add_argument('-t', '--threads', type=int, default=32, 
                       help='线程数量 (Number of threads, default: 32)')
    parser.add_argument('-s', '--speed-threshold', type=int, default=10240,
                       help='速度阈值 (Speed threshold in bytes/s, default: 10240)')
    parser.add_argument('--chunk-size', type=int, default=8192,
                       help='块大小 (Chunk size in bytes, default: 8192)')
    parser.add_argument('--timeout', type=int, default=30,
                       help='请求超时 (Request timeout in seconds, default: 30)')
    
    args = parser.parse_args()
    
    # 获取URL
    url = args.url
    if not url:
        url = input("请输入下载链接 (Enter download URL): ").strip()
    
    if not url:
        print("错误: 未提供下载链接")
        sys.exit(1)
    
    # 创建配置
    config = DownloadConfig(
        num_threads=args.threads,
        speed_threshold=args.speed_threshold,
        chunk_size=args.chunk_size,
        timeout=args.timeout
    )
    
    # 创建下载管理器并开始下载
    manager = DownloadManager(url, args.output, config)
    manager.start()


if __name__ == '__main__':
    main()
