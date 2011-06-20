import logging
import os
import re
import shutil
import tempfile
import time
import urllib2

from util import popen_results

def upload_unconverted_to_s3(youtube_id, video_path):

    s3_url = "s3://KA-youtube-unconverted/%s/%s" % (youtube_id, os.path.basename(video_path))

    command_args = ["s3cmd/s3cmd", "-c", "secrets/s3.s3cfg", "--acl-public", "put", video_path, s3_url]
    results = popen_results(command_args)
    logging.info(results)

    return s3_url

def list_converted_videos():

    videos = []
    s3_url = "s3://KA-youtube-converted"

    command_args = ["s3cmd/s3cmd", "-c", "secrets/s3.s3cfg", "ls", s3_url]
    results = popen_results(command_args)
    logging.info(results)

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
    logging.info(results)

    command_args = ["s3cmd/s3cmd", "-c", "secrets/s3.s3cfg", "--recursive", "del", s3_converted_url]
    results = popen_results(command_args)
    logging.info(results)

def download_converted_from_s3(youtube_id):

    s3_folder_url = "s3://KA-youtube-converted/%s/" % youtube_id

    temp_dir = tempfile.gettempdir()
    video_folder_path = os.path.join(temp_dir, youtube_id)

    if os.path.exists(video_folder_path):
        shutil.rmtree(video_folder_path)

    os.mkdir(video_folder_path)

    command_args = ["s3cmd/s3cmd", "-c", "secrets/s3.s3cfg", "--recursive", "get", s3_folder_url, video_folder_path]
    results = popen_results(command_args)
    logging.info(results)

    return video_folder_path

def upload_converted_to_archive(youtube_id, create_bucket=False):

    video_folder_path = download_converted_from_s3(youtube_id)
    assert(video_folder_path)
    assert(len(os.listdir(video_folder_path)))
    logging.info("Downloaded youtube id %s from s3 for archive export" % youtube_id)

    archive_bucket_url = "s3://KA-youtube-converted"

    if create_bucket:
        logging.info("Making sure archive.org bucket exists")
        command_args = ["s3cmd/s3cmd", "-c", "secrets/archive.s3cfg", "mb", archive_bucket_url]
        results = popen_results(command_args)
        logging.info(results)

        logging.info("Waiting 10 seconds")
        time.sleep(10)

    command_args = ["s3cmd/s3cmd", "-c", "secrets/archive.s3cfg", "--recursive", "--force", "put", video_folder_path, archive_bucket_url]
    results = popen_results(command_args)
    logging.info(results)

    logging.info("Waiting 10 seconds")
    time.sleep(10)

    shutil.rmtree(video_folder_path)
    logging.info("Cleaned up local video folder path")

    return verify_archive_upload(youtube_id)

def verify_archive_upload(youtube_id):

    c_retries_allowed = 3
    c_retries = 0

    while c_retries < c_retries_allowed:
        try:
            request = urllib2.Request("http://s3.us.archive.org/KA-youtube-converted/%s/%s.mp4" % (youtube_id, youtube_id))

            request.get_method = lambda: "HEAD"
            response = urllib2.urlopen(request)

            return response.code == 200
        except urllib2.HTTPError, e:
            c_retries += 1

            if c_retries < c_retries_allowed:
                logging.error("Error during archive upload verification attempt %s, trying again" % c_retries)
            else:
                logging.error("Error during archive upload verification final attempt: %s" % e)

    return False
