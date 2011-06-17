import os
import re
import shutil
import tempfile
import time

from util import popen_results

def upload_unconverted_to_s3(youtube_id, video_filename, video_path):

    s3_url = "s3://KA-youtube-unconverted/%s/%s" % (youtube_id, video_filename)

    command_args = ["s3cmd/s3cmd", "-c", "secrets/s3.s3cfg", "put", video_path, s3_url]
    results = popen_results(command_args)
    print results

    return s3_url

def clean_up_video_on_s3(youtube_id):

    s3_unconverted_url = "s3://KA-youtube-unconverted/%s/" % youtube_id
    s3_converted_url = "s3://KA-youtube-converted/%s/" % youtube_id

    command_args = ["s3cmd/s3cmd", "-c", "secrets/s3.s3cfg", "--recursive", "del", s3_unconverted_url]
    results = popen_results(command_args)
    print results

    command_args = ["s3cmd/s3cmd", "-c", "secrets/s3.s3cfg", "--recursive", "del", s3_converted_url]
    results = popen_results(command_args)
    print results

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

def download_from_s3(youtube_id, s3_folder_url):

    temp_dir = tempfile.gettempdir()

    video_folder_path = os.path.join(temp_dir, "%s" % youtube_id)

    if os.path.exists(video_folder_path):
        shutil.rmtree(video_folder_path)

    os.mkdir(video_folder_path)

    command_args = ["s3cmd/s3cmd", "-c", "secrets/s3.s3cfg", "--recursive", "get", s3_folder_url, video_folder_path]
    results = popen_results(command_args)
    print results

    return video_folder_path

def upload_converted_to_archive(youtube_id, video_folder_path):

    archive_bucket_url = "s3://KA-converted"

    command_args = ["s3cmd/s3cmd", "-c", "secrets/archive.s3cfg", "mb", archive_bucket_url]
    results = popen_results(command_args)
    print results

    time.sleep(10)
    print "Waited 10 seconds"

    command_args = ["s3cmd/s3cmd", "-c", "secrets/archive.s3cfg", "--recursive", "--force", "put", video_folder_path, archive_bucket_url]
    results = popen_results(command_args)
    print results

    return archive_bucket_url
