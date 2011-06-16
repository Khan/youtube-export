import re
import tempfile
import os
import shutil

from util import popen_results

def upload_unconverted_to_s3(youtube_id, video_filename, video_path):

    s3_url = "s3://KA-youtube-unconverted/%s/%s" % (youtube_id, video_filename)

    command_args = ["s3cmd/s3cmd", "-c", "secrets/s3.s3cfg", "put", video_path, s3_url]
    results = popen_results(command_args)
    print results

    return s3_url

def list_converted_videos():

    videos = []
    s3_url = "s3://KA-youtube-converted"

    command_args = ["s3cmd/s3cmd", "-c", "secrets/s3.s3cfg", "ls", s3_url]
    results = popen_results(command_args)
    print results

    regex = re.compile("s3://KA-youtube-converted/(.+)/")

    for match in regex.finditer(results):
        videos.append({
                "url": match.group(),
                "youtube_id": match.groups()[0]
            })

    return videos

def upload_converted_to_archive(youtube_id, video_filename, video_path):
    pass

def download_from_s3(youtube_id, s3_folder_url):

    temp_dir = tempfile.gettempdir()

    video_folder_path = os.path.join(temp_dir, "%s-converted" % youtube_id)

    if os.path.exists(video_folder_path):
        shutil.rmtree(video_folder_path)

    os.mkdir(video_folder_path)

    command_args = ["s3cmd/s3cmd", "-c", "secrets/s3.s3cfg", "--recursive", "get", s3_folder_url, video_folder_path]
    results = popen_results(command_args)
    print results

    return video_folder_path
