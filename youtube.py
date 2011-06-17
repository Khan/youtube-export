import tempfile
import os
from util import popen_results

def download(video):

    temp_dir = tempfile.gettempdir()

    youtube_id = video["youtube_id"]
    youtube_url = video["url"]

    video_filename = youtube_id + ".flv"
    video_path = os.path.join(temp_dir, video_filename)

    if os.path.exists(video_path):
        os.remove(video_path)

    command_args = ["python", "youtube-dl/youtube-dl.py", "--max-quality", "22", "-icw", "-o", video_path, youtube_url]
    results = popen_results(command_args)
    print results

    return (youtube_id, video_filename, video_path)
