import os
import re
import shutil
import tempfile
import time

from util import popen_results

def upload_unconverted_to_s3(youtube_id, video_path):

    s3_url = "s3://KA-youtube-unconverted/%s/%s" % (youtube_id, os.path.basename(video_path))

    command_args = ["s3cmd/s3cmd", "-c", "secrets/s3.s3cfg", "--acl-public", "put", video_path, s3_url]
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

def clean_up_video_on_s3(youtube_id):

    s3_unconverted_url = "s3://KA-youtube-unconverted/%s/" % youtube_id
    s3_converted_url = "s3://KA-youtube-converted/%s/" % youtube_id

    command_args = ["s3cmd/s3cmd", "-c", "secrets/s3.s3cfg", "--recursive", "del", s3_unconverted_url]
    results = popen_results(command_args)
    print results

    command_args = ["s3cmd/s3cmd", "-c", "secrets/s3.s3cfg", "--recursive", "del", s3_converted_url]
    results = popen_results(command_args)
    print results
