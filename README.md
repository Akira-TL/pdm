# PDM — 模拟 IDM 下载流程的 Python 工具

> NOTE:  
> 以下内容为 AI 自动生成，并不代表全部功能  
> 等软件写完了基本稳定了就手写

一个使用 Python 实现的异步多段下载器，支持多连接分块下载、断点续传、下载进度展示、低速分片自动重启、批量任务等功能。

当前实现基于 asyncio + aiohttp + aiofiles + rich + loguru，所有行为均以源码 [pdm.py](pdm.py) 为准。

---

## 功能概览

- 多连接分块下载：同一 URL 通过 Range 头拆分为多个分块并发下载。
- 断点续传：每个任务使用 `.pdm.<sha>` 目录保存分块和 `.pdm` 元信息，配合 `--continue` 恢复下载。
- 下载进度展示：使用 rich 展示每个文件的进度条、速度、已用时间和剩余时间。
- 失败重试：支持整体任务重试（`--retry`）和分块级重试 + 低速重启（`--chunk-retry-speed`）。
- 并发控制：
  - `--max-downloads` 控制同时下载的 URL 个数；
  - `--max-concurrent-downloads` 控制单个 URL 内部并发的分块数；
  - `--force-sequential` 强制单 URL 顺序下载（相当于将 `max_concurrent_downloads` 设为 1）。
- 日志：
  - 全局日志输出到终端或指定文件；
  - 每个 URL 额外有一个独立的 `.pdm.<sha>.log` 日志（位于对应下载目录内）。
- 完整性校验（MD5）：
  - 若任务中提供 `md5` 字段并启用 `--check-integrity`，下载完成后会对合并后的文件做 MD5 校验。
  - `md5` 可为 32 位 MD5 字符串、本地文件路径或一个返回 MD5 字符串的 URL。
- 批量下载任务文件：支持 JSON / YAML / 纯文本三种格式。

---

## 安装依赖

```bash
pip install aiohttp aiofiles rich loguru yarl pyyaml
```

> 说明：
>
> - `aiohttp` / `aiofiles`：异步 HTTP 与文件读写
> - `rich`：进度条与终端输出
> - `loguru`：日志
> - `yarl`：URL 处理
> - `pyyaml`：解析 YAML 任务文件

---

## 快速开始

### 1. 单 URL 下载

```bash
python pdm.py "https://example.com/file.bin"
```

行为说明：

- 输出目录：默认当前工作目录。
- 文件名：优先使用服务器返回的 `Content-Disposition` 中的 `filename`；否则取 URL 路径末尾；若无法获取则使用 URL 的哈希值生成一个 `.dat` 文件名。
- 分块与临时文件：
  - 会在目标目录下创建一个 `.pdm.<sha>` 目录（`<sha>` 为 URL 的短哈希），
  - 其中保存所有分块文件以及一个 `.pdm` 元信息文件。

下载完成后，分块会被合并为最终文件，`.pdm.<sha>` 目录会被删除。

### 2. 断点续传

按下 Ctrl+C 或进程异常结束后，可以使用 `--continue` 继续下载：

```bash
python pdm.py --continue "https://example.com/file.bin"
```

行为：

- 若对应的 `.pdm.<sha>` 目录和 `.pdm` 元数据存在且信息匹配（URL、文件名、文件大小、MD5 等），则会在现有分块基础上继续下载。
- 如果元数据与当前任务不一致，则会清空该临时目录并重新开始。

### 3. 批量下载

使用 `-i/--input-file` 可以从文件中读取任务。该参数可以重复出现，多个文件会顺序加载。

#### 3.1 纯文本

`urls.txt`：

```text
https://example.com/a.iso
https://example.com/b.zip
```

执行：

```bash
python pdm.py -i urls.txt
```

每个 URL 会使用当前工作目录作为下载目录，文件名按前文规则自动推断。

#### 3.2 JSON 任务文件

JSON 结构为「URL 作为键，对应配置作为值」：

```json
{
  "https://example.com/a.iso": {
    "file_name": "linux.iso",
    "dir_path": "/data/downloads",
    "md5": "0123456789abcdef0123456789abcdef",
    "log_path": "/data/logs/a.log"
  },
  "https://example.com/b.zip": {
    "dir_path": "/data/downloads/b",
    "md5": "https://example.com/b.zip.md5"
  }
}
```

```bash
python pdm.py -i tasks.json
```

字段说明：

- `file_name`：最终合并后的文件名（可选）。
- `dir_path`：该 URL 的下载目录（可选，不填则为当前工作目录）。
- `md5`：
  - 32 位 MD5 字符串，或
  - 本地文件路径（从文件读取 MD5），或
  - 以 http/https/ftp 开头的 URL（从响应内容读取 MD5）。
- `log_path`：该 URL 的日志文件路径（可选）。

#### 3.3 YAML 任务文件

YAML 结构与 JSON 类似：

```yaml
https://example.com/a.iso:
  file_name: linux.iso
  dir_path: /data/downloads
  md5: 0123456789abcdef0123456789abcdef
  log_path: /data/logs/a.log

https://example.com/b.zip:
  dir_path: /data/downloads/b
  md5: https://example.com/b.zip.md5
```

执行：

```bash
python pdm.py -i tasks.yaml
```

---

## 下载行为与分块策略

- 文件大小获取：
  - 先通过 HEAD 请求读取 `Content-Length`；若不存在则记为 `-1`（表示未知大小）。
- 初始分块：
  - 默认将文件按 `max_concurrent_downloads` 等分；
  - 分块大小不少于 `min_split_size`（`-k/--min-split-size`），并对齐到 10 KiB 的整数倍。
- 动态分块：
  - 下载过程中，程序会在空隙较大的分块中间再拆分出新的分块，提高利用率。
- 低速重启：
  - 每个分块在下载时会统计瞬时速度；
  - 若 `--chunk-retry-speed` 设置了阈值，且速度低于该值，会中断当前分块请求，稍后重试该分块。

---

## 命令行参数（以 pdm.py 为准）

> 下表中「状态」列说明：
>
> - [x] 已接线并在当前代码中生效。
> - ⚠️ 参数存在，但当前实现中尚未真正使用（预留）。

### 通用

- [x] `-v, --version`：打印版本号（`0.1.1`）后退出。
- [x] `-l, --log PATH`：日志文件路径；若为 `-`，日志输出到标准输出。
- [x] `--debug`：启用调试模式，日志级别提升为 DEBUG。

### 下载目标与输出

- ⚠️ `-d, --dir DIR`：设计为指定下载目录，但当前代码中 **未使用该参数**；请改用任务文件中的 `dir_path` 字段控制。
- ⚠️ `-o, --out NAME`：设计为指定输出文件名，但当前版本中尚未在下载逻辑中使用，等价于无效。
- [x] 位置参数 `urls...`：要下载的 URL，可以传多个。

### 下载控制

- [x] `-N, --max-downloads INT`：同时下载的 URL 最大数量（默认 4）。
- [x] `-x, --max-concurrent-downloads INT`：每个 URL 内部并发的分块下载数量（默认 5）。
- [x] `-Z, --force-sequential`：强制顺序下载：将单个 URL 的并发数限制为 1。
- [x] `-k, --min-split-size SIZE`：分块最小尺寸（默认 `1M`；支持 K/M/G 后缀）。当文件较小或 SIZE 过大时，可能只会产生一个分块。
- [x] `-r, --retry INT`：整体任务与分块级别的重试次数（默认 3）。
- [x] `-W, --retry-wait SECONDS`：重试前的等待时间（默认 5 秒）。
- [x] `--timeout SECONDS`：HEAD 请求和部分其他网络操作的超时时间（默认 60 秒）。
- [x] `--chunk-timeout SECONDS`：每个分块下载请求的超时时间（默认 10 秒）。
- [x] `--chunk-retry-speed SIZE`：当分块下载速度低于该值（字节/秒）时，会重启该分块的下载；支持 K/M/G 后缀。留空则不启用此机制。
- [x] `-c, --continue`：启用断点续传，根据 `.pdm` 元信息和已有分块续传。
- [x] `--tmp DIR`：指定临时分块目录的根路径；不指定时，默认在对应下载目录下创建 `.pdm.<sha>`。

### 完整性与校验

- [x] `-V, --check-integrity`：启用 MD5 完整性校验。仅当任务文件中为该 URL 提供了 `md5` 字段时才会实际校验。

### 网络与 UA

- ⚠️ `-ua, --user-agent STRING`：当前代码只在获取文件名的 HEAD 请求中设置 UA，真正的下载请求尚未使用该 UA。默认值：`PDM-Downloader/1.0`。

### 批量任务与日志

- [x] `-i, --input-file FILE`：从 FILE 读取下载任务；支持 JSON/YAML/纯文本，参数可重复。
- [x] `--auto-file-renaming BOOL`：若目标目录下已存在同名文件，则自动追加 `.1`、`.2` 等序号进行重命名（默认 `True`）。

### 预留 / 尚未实现的能力

以下能力目前在 README / 代码注释中有规划，但 **尚未在当前版本中实现对应的命令行参数或逻辑**：

- 自定义任意 Header（如 `-H "Key: Value"`）。
- 代理与 TLS 相关设置（如 `--proxy`、`--insecure` 等）。
- 全局或每线程带宽限速（如 `--limit`、`--limit-per-thread`）。
- 显式的状态文件路径配置（如 `--state-file`）。

这些功能如后续实现，会同步更新 README 与帮助信息。

---

## 当前进度（概览）

- [x] 异步下载框架与分块调度
- [x] rich 进度条与 loguru 日志集成
- [x] 分块拆分、动态分块与最小分块尺寸控制
- [x] 断点续传元信息与分块重建
- [x] 批量任务（JSON / YAML / 纯文本）
- [x] MD5 完整性校验（基于任务文件中的 md5 字段）
- [x] 分块下载低速自动重启策略（`--chunk-retry-speed`）
- [x] 下载与分块层面的重试与等待（`--retry`、`--retry-wait`）
- [-] 全局 `-d/--dir`、`-o/--out`、`-ua/--user-agent` 等仍在逐步完善接线
- [ ] 代理 / TLS / 限速等进阶功能尚未实现

---

## 源码

- 主程序： [pdm.py](pdm.py)
