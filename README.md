# PDM — 模拟 IDM 下载流程的 Python 工具

一个使用 Python 实现的多线程断点续传下载器，力求模拟 IDM（Internet Download Manager）的工作方式。支持多线程分块下载、下载进度持久化、断点续传、线程低速自动重启等能力，并计划支持更多高级功能，如自定义 UA、限速、不可续传链接处理等。

仓库描述：python packages simulate the IDM  
语言组成：Python 100%

---

## 目标

- 模拟 IDM 下载核心流程：获取链接、设置请求头、发送请求、分块下载、断点续传、合并与完成。
- 支持高并发多线程。
- 断点续传：下载状态持久化，进程重启后可继续。
- 速度监控与自动重启：某线程速度低于阈值持续一段时间时自动重启该线程并从断点继续。
- 自定义请求头与 UA，支持代理、超时、重试等网络配置。
- 不可续传链接的降级与兼容策略。
- 下载限速以控制带宽占用。
- 可扩展的批量下载与并发队列控制。

---

## 使用方法

### 安装依赖

```bash
# 安装基础依赖
pip install aiohttp aiofiles requests rich loguru yarl multidict pyyaml
```

### 基本下载（32 线程，断点续传）

```bash
python pdm.py "https://example.com/file.bin" --threads 32 --continue --out file.bin
```

### 自定义 UA 与 Headers

```bash
python pdm.py "https://example.com/file.bin" --ua "PDM/0.1.1 (+https://github.com/Akira-TL/pdm)" \
  -H "Accept: */*" -H "X-Trace: 123"
```

### 限速（全局 5MB/s，每线程 512KB/s），低速重启策略

```bash
python pdm.py "https://example.com/file.bin" --limit 5M/s --limit-per-thread 512K/s \
  --low-speed-threshold 10K/s --low-speed-time 8
```

### 不可续传降级：单线程流式下载

```bash
python pdm.py "https://example.com/stream" --no-range-fallback --unknown-size-buffer 1M
```

### 批量下载队列与并发控制

```bash
python pdm.py --input-file urls.txt --max-concurrent-downloads 3 --threads 16
```

---

## 已完成/未完成

- [x] 基础框架搭建
- [x] 进度展示
- [x] 异步网络栈依赖
- [x] 多线程/多段下载的参数设计
- [ ] 下载任务较多情况下多进程下载处理
- [x] 自定义 UA 与通用 Headers 的 CLI 支持与请求集成
- [ ] 下载限速
- [ ] 不可续传链接降级策略与恢复逻辑
- [x] 低速阈值监控与自动重启线程
- [ ] 断点续传状态文件定义与稳定持久化
- [x] 动态分块与最小分块尺寸策略
- [x] 批量下载与队列并发
- [x] 完整日志等级与输出
- [x] 完整性校验
- [ ] 代理与 TLS 配置
- [ ] 单元/集成测试
- [ ] 打包与发布

---

## 参数说明

### 已定义

- 基本

  - [x] `-o, --output PATH` 输出文件路径（或目录）
  - [x] `-t, --threads INT` 线程数（默认 32）
  - [x] `--continue` 断点续传开关（默认开启）
  - [x] `--split-size SIZE` 每块最小尺寸（如 `20M`；支持 `K/M/G`）
  - [x] `--timeout SECONDS` 超时（默认 10）
  - [x] `--retry INT` 重试次数（默认 3）

- 请求与网络

  - [x] `--ua STRING` 自定义 User-Agent
  - [ ] `-H, --header "Key: Value"` 可多次传入任意请求头
  - [ ] `--proxy URL` 代理（如 `http://127.0.0.1:7890`）
  - [ ] `--insecure` 关闭 TLS 验证（不推荐）

- 限速与监控

  - [ ] `--limit RATE` 全局限速（如 `5M`）
  - [ ] `--limit-per-thread RATE` 每线程限速
  - [x] `--low-speed-threshold RATE` 低速阈值（触发重启）
  - [ ] `--low-speed-time SECONDS` 低速持续时间（判定窗口）

- 不可续传策略

  - [x] `--no-range-fallback` 遇到不支持 Range 时，自动切换单线程顺序下载
  - [ ] `--unknown-size-buffer SIZE` 流式写入缓冲（如 `1M`）

- 进度与状态
  - [ ] `--state-file PATH` 状态文件路径（默认同目录：`<output>.state.json`）
  - [x] `--log-level LEVEL` 日志级别（`INFO/DEBUG/WARN/ERROR`）
  - [x] `--input-file PATH` 批量 URL 列表（每行一个 URL）
  - [x] `--max-concurrent-downloads INT` 批量下载并发上限

## 参考文件

- 源码入口：[pdm.py](https://github.com/Akira-TL/pdm/blob/main/pdm.py)
