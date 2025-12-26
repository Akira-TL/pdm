#!/usr/bin/env python3
"""
功能演示脚本 - Feature Demonstration Script

演示下载管理器的核心功能（无需实际下载）
Demonstrates core features of the download manager (without actual downloads)
"""

import os
import sys
import json
import tempfile
from pathlib import Path

# 添加当前目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from download_manager import DownloadConfig, DownloadThread, DownloadManager


def print_header(title):
    """打印标题"""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def demo_1_config():
    """演示1: 配置系统"""
    print_header("演示1: 灵活的配置系统")
    
    print("\n默认配置:")
    config_default = DownloadConfig()
    print(f"  - 线程数: {config_default.num_threads}")
    print(f"  - 速度阈值: {config_default.speed_threshold} 字节/秒 ({config_default.speed_threshold/1024:.1f} KB/s)")
    print(f"  - 块大小: {config_default.chunk_size} 字节 ({config_default.chunk_size/1024:.1f} KB)")
    print(f"  - 超时: {config_default.timeout} 秒")
    print(f"  - 最大重试: {config_default.max_retries} 次")
    
    print("\n自定义高性能配置 (128线程):")
    config_high = DownloadConfig(num_threads=128, speed_threshold=51200, chunk_size=16384)
    print(f"  - 线程数: {config_high.num_threads}")
    print(f"  - 速度阈值: {config_high.speed_threshold} 字节/秒 ({config_high.speed_threshold/1024:.1f} KB/s)")
    print(f"  - 块大小: {config_high.chunk_size} 字节 ({config_high.chunk_size/1024:.1f} KB)")
    
    print("\n✓ 配置系统支持完全自定义")


def demo_2_thread_management():
    """演示2: 线程管理"""
    print_header("演示2: 智能线程管理")
    
    url = "https://example.com/test.zip"
    output = "/tmp/test.zip"
    config = DownloadConfig(num_threads=4)
    
    print("\n创建下载管理器...")
    manager = DownloadManager(url, output, config)
    manager.total_size = 1024 * 1024  # 1MB
    manager._initialize_threads()
    
    print(f"文件大小: {manager.total_size} 字节 (1 MB)")
    print(f"线程数: {len(manager.threads)}")
    print("\n线程分配:")
    
    for i, thread in enumerate(manager.threads):
        size = thread.end - thread.start + 1
        print(f"  线程 {i}: 字节 {thread.start}-{thread.end} ({size/1024:.1f} KB)")
    
    print("\n✓ 自动将文件分割成相等的块，每个线程处理一块")


def demo_3_state_persistence():
    """演示3: 状态持久化"""
    print_header("演示3: 断点续传 - 状态持久化")
    
    # 创建临时目录
    with tempfile.TemporaryDirectory() as tmpdir:
        state_file = os.path.join(tmpdir, "test.zip.state")
        
        print("\n模拟保存下载状态...")
        state = {
            'url': 'https://example.com/test.zip',
            'output_path': '/tmp/test.zip',
            'total_size': 1048576,
            'num_threads': 4,
            'threads': [
                {
                    'thread_id': 0,
                    'start': 0,
                    'end': 262143,
                    'current_pos': 150000,
                    'completed': False,
                    'downloaded': 150000,
                    'speed': 2048.5
                },
                {
                    'thread_id': 1,
                    'start': 262144,
                    'end': 524287,
                    'current_pos': 524287,
                    'completed': True,
                    'downloaded': 262144,
                    'speed': 0
                },
                {
                    'thread_id': 2,
                    'start': 524288,
                    'end': 786431,
                    'current_pos': 600000,
                    'completed': False,
                    'downloaded': 75712,
                    'speed': 1536.2
                },
                {
                    'thread_id': 3,
                    'start': 786432,
                    'end': 1048575,
                    'current_pos': 800000,
                    'completed': False,
                    'downloaded': 13568,
                    'speed': 512.8
                }
            ]
        }
        
        with open(state_file, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=2)
        
        print(f"状态已保存到: {state_file}")
        print("\n状态内容:")
        print(f"  - URL: {state['url']}")
        print(f"  - 总大小: {state['total_size']} 字节")
        print(f"  - 线程数: {state['num_threads']}")
        
        total_downloaded = sum(t['downloaded'] for t in state['threads'])
        progress = (total_downloaded / state['total_size']) * 100
        
        print(f"\n进度信息:")
        print(f"  - 已下载: {total_downloaded} / {state['total_size']} 字节")
        print(f"  - 进度: {progress:.2f}%")
        
        print("\n各线程状态:")
        for t in state['threads']:
            status = "✓ 完成" if t['completed'] else "↻ 进行中"
            print(f"  线程 {t['thread_id']}: {status} - "
                  f"{t['current_pos']}/{t['end']} - "
                  f"速度: {t['speed']/1024:.2f} KB/s")
        
        print("\n✓ 脚本重启后可从此状态恢复下载")


def demo_4_speed_monitoring():
    """演示4: 速度监控"""
    print_header("演示4: 智能速度监控与自动重启")
    
    print("\n速度监控机制:")
    print("  1. 每个线程独立计算下载速度")
    print("  2. 监控线程每5秒检查一次所有线程")
    print("  3. 如果线程速度低于阈值（默认10KB/s）且已运行超过10秒")
    print("  4. 自动重启该慢速线程")
    
    print("\n示例场景:")
    config = DownloadConfig(speed_threshold=10240)  # 10KB/s
    print(f"  阈值设置: {config.speed_threshold/1024:.1f} KB/s")
    
    scenarios = [
        ("线程1", 25600, "正常运行"),
        ("线程2", 15360, "正常运行"),
        ("线程3", 5120, "速度过慢 → 自动重启"),
        ("线程4", 8192, "速度过慢 → 自动重启"),
    ]
    
    print("\n当前状态:")
    for name, speed, action in scenarios:
        status = "✓" if speed >= config.speed_threshold else "⚠"
        print(f"  {status} {name}: {speed/1024:.1f} KB/s - {action}")
    
    print("\n✓ 自动优化下载性能，确保所有线程高效工作")


def demo_5_error_handling():
    """演示5: 错误处理"""
    print_header("演示5: 健壮的错误处理")
    
    config = DownloadConfig(max_retries=3)
    
    print("\n错误处理机制:")
    print(f"  - 每个线程独立重试，最多 {config.max_retries} 次")
    print("  - 一个线程失败不影响其他线程")
    print("  - 网络错误自动重试，带退避延迟")
    print("  - 保存状态以便后续恢复")
    
    print("\n支持的错误场景:")
    error_scenarios = [
        "网络连接超时",
        "HTTP错误（4xx, 5xx）",
        "连接中断",
        "磁盘空间不足",
        "文件权限问题"
    ]
    
    for i, scenario in enumerate(error_scenarios, 1):
        print(f"  {i}. {scenario}")
    
    print("\n✓ 完善的错误处理确保下载的可靠性")


def demo_6_file_merging():
    """演示6: 文件合并"""
    print_header("演示6: 自动文件合并")
    
    print("\n文件合并流程:")
    print("  1. 所有线程完成下载")
    print("  2. 按线程ID顺序读取所有.part文件")
    print("  3. 合并到最终输出文件")
    print("  4. 删除所有临时.part文件")
    print("  5. 删除.state状态文件")
    
    print("\n示例:")
    print("  输入文件:")
    part_files = [
        "  - output.zip.part0 (256 KB)",
        "  - output.zip.part1 (256 KB)",
        "  - output.zip.part2 (256 KB)",
        "  - output.zip.part3 (256 KB)"
    ]
    for pf in part_files:
        print(pf)
    
    print("\n  合并后:")
    print("  → output.zip (1024 KB)")
    
    print("\n  清理:")
    print("  ✓ 删除 output.zip.part0")
    print("  ✓ 删除 output.zip.part1")
    print("  ✓ 删除 output.zip.part2")
    print("  ✓ 删除 output.zip.part3")
    print("  ✓ 删除 output.zip.state")
    
    print("\n✓ 自动化的文件管理，用户无需手动处理")


def main():
    """主函数"""
    print("\n" + "╔" + "═" * 68 + "╗")
    print("║" + " " * 15 + "多线程断点续传下载管理器" + " " * 15 + "║")
    print("║" + " " * 23 + "功能演示" + " " * 23 + "║")
    print("╚" + "═" * 68 + "╝")
    
    # 运行所有演示
    demo_1_config()
    demo_2_thread_management()
    demo_3_state_persistence()
    demo_4_speed_monitoring()
    demo_5_error_handling()
    demo_6_file_merging()
    
    print("\n" + "=" * 70)
    print("  总结")
    print("=" * 70)
    
    print("\n本下载管理器实现了以下核心功能:")
    features = [
        "✓ 多线程并发下载（默认32线程，可配置）",
        "✓ 断点续传（自动保存和恢复状态）",
        "✓ 智能速度监控和自动重启",
        "✓ HTTP Range请求支持",
        "✓ 自动文件合并",
        "✓ 完善的错误处理和重试",
        "✓ 实时进度显示",
        "✓ 详细日志记录"
    ]
    
    for feature in features:
        print(f"  {feature}")
    
    print("\n使用方法:")
    print("  python download_manager.py <URL> [选项]")
    print("  python download_manager.py --help  # 查看所有选项")
    
    print("\n" + "=" * 70 + "\n")


if __name__ == '__main__':
    main()
