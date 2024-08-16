import asyncio
from web_http import download_file


if __name__ == "__main__":
    # s_url = "https://cdn.zoomg.ir/2023/11/the-bikeriders-cast.jpg"
    # s_url = "https://examplefile.com/file-download/15"
    s_url = "https://file-examples.com/storage/fe519944ff66ba53b99c446/2017/04/file_example_MP4_480_1_5MG.mp4"
    try:
        asyncio.run(download_file(url=s_url))
    except KeyboardInterrupt:
        print("download cancelled...")