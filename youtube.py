import tempfile
import os
from util import popen_results

def download(video):

    temp_dir = tempfile.mkdtemp()

    youtube_id = video["youtube_id"]
    youtube_url = video["url"]

    video_filename_template = youtube_id + ".%(ext)s"
    video_path_template = os.path.join(temp_dir, video_filename_template)

    command_args = ["python", "youtube-dl/youtube-dl.py", "--max-quality", "22", "-icw", "-o", video_path_template, youtube_url]
    results = popen_results(command_args)
    print results

    files = os.listdir(temp_dir)
    assert len(files) == 1
    video_path = os.path.join(temp_dir, files[0])
    print video_path

    return (youtube_id, video_path)
