#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""
CLI 入口：提供命令行解析并调用 PDManager。
安装后可通过 console_scripts 生成可执行命令。
"""

import os
import sys
import argparse
import asyncio
from .manager import PDManager

version = "0.1.1"


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"pdm-downloader version {version}",
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
        help="Check file integrity by validating piece hashes or a hash of the entire file.",
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
        help="Downloads URIs found in FILE(s). Supports JSON, YAML, or plain text.",
    )
    parser.add_argument(
        "-x",
        "--max-concurrent-downloads",
        type=int,
        default=5,
        help="Set maximum number of parallel downloads for each URL or task.",
    )
    parser.add_argument(
        "--chunk-retry-speed",
        default="",
        help="If the chunk speed falls below SIZE bytes/second, restart that chunk. Append K/M.",
    )
    parser.add_argument(
        "-r",
        "--retry",
        type=int,
        default=3,
        help="Number of times to retry downloading a URL upon failure.",
    )
    parser.add_argument(
        "-W",
        "--retry-wait",
        type=int,
        default=5,
        help="Maximum wait time in seconds between retries.",
    )
    parser.add_argument(
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
        help="Disable auto renaming when target file exists.",
    )
    parser.add_argument(
        "-Z",
        "--force-sequential",
        action="store_true",
        help="Fetch URIs sequentially.",
    )
    parser.add_argument(
        "-k",
        "--min-split-size",
        type=str,
        default="1M",
        help="Minimum split size. Append K/M.",
    )
    parser.add_argument(
        "--tmp",
        type=str,
        default=None,
        help="Temporary directory for chunk files.",
    )
    parser.add_argument(
        "-t",
        "--threads",
        type=int,
        default=4,
        help="Alias of max-downloads (deprecated).",
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
        help="The URL(s) to download.",
    )

    args = parser.parse_args(argv)
    if args.log == "-":
        args.log = sys.stdout
    if args.force_sequential and args.out is not None:
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

    if args.urls and len(args.urls) == 1 and args.out is not None:
        pdm.append(args.urls[0], file_name=args.out)
    else:
        if args.out is not None:
            pass  # ignore --out when multiple urls
        pdm.add_urls(args.urls or [])

    if args.input_file:
        for file in args.input_file:
            if os.path.exists(file):
                pdm.load_input_file(file)

    asyncio.run(pdm.start_download())


if __name__ == "__main__":
    main()
