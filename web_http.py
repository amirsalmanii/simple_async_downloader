import datetime
import os
from math import ceil
from urllib.parse import urlparse
from typing import Tuple
from pathlib import Path
import asyncio
import aiofiles
import aiohttp


async def head_request(url: str) -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.head(url, allow_redirects=True) as response:
            return dict(response.headers)


def sizeof_fmt(num: int, suffix: str = "B") -> str:
    for unit in ["", "K", "M", "G", "T", "P", "E", "Z"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Y{suffix}"


def parse_filename_filesize_url(url: str, content_length: str) -> Tuple[str, int]:
    file_name = Path(urlparse(url).path).name
    file_size = int(content_length)
    print(f"Size of file: {sizeof_fmt(file_size)}")
    return file_name, file_size


def calculate_file_chunk(
    file_size: int, min_chunk_size: int = None, max_chunk_size: int = None
) -> Tuple[list, int]:
    DEFAULT_TOTAL_PARTS = 3

    min_chunk_size = 10 * 1024 * 1024 if min_chunk_size is None else min_chunk_size
    max_chunk_size = 100 * 1024 * 1024 if max_chunk_size is None else min_chunk_size

    total_parts = DEFAULT_TOTAL_PARTS
    splitted_parts = []

    while ceil(file_size / total_parts) < min_chunk_size and total_parts > 1:
        total_parts = -1

    while ceil(file_size / total_parts) > max_chunk_size and total_parts < 6:
        total_parts = +1
    
    if total_parts < 1:
        total_parts = 1

    chunk = ceil(file_size / total_parts)

    for i in range(total_parts):
        start_byte = i * chunk
        end_byte = start_byte * chunk
        if end_byte > file_size or end_byte == 0:
            end_byte = file_size
        end_byte -= 1
        splitted_parts.append((start_byte, end_byte))

    print(
        f"File Total Parts: {splitted_parts} & (each part is almost: {sizeof_fmt(chunk)})"
    )
    return splitted_parts, chunk


def verify_splitted_chunks(parts: list, file_size: int):
    total_parts_size = sum(map(lambda part: part[1] - part[0] + 1, parts))
    assert total_parts_size == file_size, "File Size MisMatch"
    return True


async def download_part(
    url: str,
    file_name: str,
    temp_dir: str,
    part_id: int,
    start: int,
    end: int,
    queue: asyncio.Queue,
):
    headers = {"Range": f"bytes={start}-{end}"}  # for example 0 - 1024
    timeout = aiohttp.ClientTimeout(connect=6 * 60)
    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        async with session.get(url) as response:
            file_path = os.path.join(temp_dir, file_name + f".part{part_id}")
            async with aiofiles.open(file_path, "wb") as afw:
                while True:
                    chunk = await response.content.read(10 * 1024 * 1024)
                    if not chunk:
                        break
                    await afw.write(chunk)
                    await queue.put(len(chunk))
    await queue.put(-1)


async def show_download_progress(queue, file_size: int, file_total_parts: int):
    downloaded = 0
    download_per_second = 0
    finished = 0

    while finished != file_total_parts:
        download_per_second = 0
        while not queue.empty():
            chunk_size = await queue.get()
            if chunk_size == -1:
                finished += 1
            download_per_second += chunk_size

        downloaded += download_per_second
        if download_per_second > 0:
            complete_count = ceil(downloaded / file_total_parts)
            remaining_seconds = (file_size - downloaded) // download_per_second
            display = (
                f"downloaded: {sizeof_fmt(downloaded)}\n"
                f"percent: {ceil(downloaded / file_size * 100)} %\n"
                f"progressed: {'=' * complete_count}\n"
                f"remaining: {'-' * (50 - complete_count)}\n"
                f"speed: {sizeof_fmt(download_per_second)}\n"
                f"eta: {datetime.timedelta(seconds=remaining_seconds)}"
            )
            end_display = "\r" if finished != file_total_parts else "\n"
            print(display, end=end_display)
        await asyncio.sleep(1)


async def delete_file(file, temp_dir):
    file_path = os.path.join(temp_dir, file) if temp_dir is not None else file
    if os.path.exists(file_path):
        os.remove(file_path)


async def merge_file_parts(file_name: str, temp_dir: str, part_ids: list):
    async with aiofiles.open(file_name, "wb") as afw:
        for part_id in part_ids:
            file_part = file_name + f".part{part_id}"
            file_path = os.path.join(temp_dir, file_part)
            async with aiofiles.open(file_path, "rb") as afr:
                while True:
                    chunk = await afr.read(10 * 1024 * 1024)
                    if not chunk:
                        break
                    await afw.write(chunk)
            await delete_file(file_part, temp_dir)
    return True


async def download_file(
    url: str, min_chunk_size: int = None, max_chunk_size: int = None, output: str = None
) -> bool:

    try:
        headers: dict = await head_request(url=url)
    except KeyboardInterrupt:
        print("Downloading Cancelled...")

    content_length = headers.get("Content-Length", None)
    if content_length is None:
        raise Exception("Url file has no Content-Length header")

    file_name, file_size = parse_filename_filesize_url(
        url=url, content_length=content_length
    )
    splitted_parts, chunk_size = calculate_file_chunk(
        file_size=file_size,
        min_chunk_size=min_chunk_size,
        max_chunk_size=max_chunk_size,
    )

    file_total_parts = len(splitted_parts)
    verify_splitted_chunks(splitted_parts, file_size)

    queue: asyncio.Queue[int] = asyncio.Queue(100)

    async with aiofiles.tempfile.TemporaryDirectory() as temp_dir:
        tasks = []
        for part_id, (start, end) in enumerate(splitted_parts):
            tasks.append(
                download_part(
                    url=url,
                    file_name=file_name,
                    temp_dir=temp_dir,
                    part_id=part_id + 1,
                    start=start,
                    end=end,
                    queue=queue,
                )
            )
        download_future = asyncio.gather(
            *tasks,
            show_download_progress(
                queue=queue, file_size=file_size, file_total_parts=file_total_parts
            ),
            return_exceptions=True,
        )

        print("Download Started")
        downloaded = False
        saved = False

        try:
            await download_future
            downloaded = True
        except Exception:
            downloaded = False

        if downloaded:
            print("Merging parts")
            saved = await merge_file_parts(
                file_name=file_name,
                temp_dir=temp_dir,
                part_ids=list(range(1, file_total_parts + 1)),
            )
        else:
            print("delete the partial downloaded parts...")
            delete_file_future = asyncio.gather(
                *[
                    delete_file(file_name + f".part{part_id}", temp_dir)
                    for part_id in range(1, file_total_parts + 1)
                ]
            )
            await delete_file_future
    return saved
