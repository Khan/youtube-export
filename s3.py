import logging
import os
import re
import shutil
import tempfile
import time
import unicodedata
import urllib2
from os.path import splitext
from collections import defaultdict
from boto.s3.connection import S3Connection, OrdinaryCallingFormat
from boto.s3.key import Key
from secrets import s3_access_key, s3_secret_key
from util import logger

# We use bucket names with uppercase characters, so we must use OrdinaryCallingFormat
# instead of the default SubdomainCallingFormat
connection = S3Connection(s3_access_key, s3_secret_key, calling_format=OrdinaryCallingFormat())

converted_bucket = connection.get_bucket("KA-youtube-converted")
unconverted_bucket = connection.get_bucket("KA-youtube-unconverted")

# Keys (inside buckets) are in the format YOUTUBE_ID.FORMAT
# e.g. DK1lCc9b7bg.mp4/ or Dpo_-GrMpNE.m3u8/
re_video_key_name = re.compile(r"([\w-]+)\.(\w+)/")

# Older keys are of the form YOUTUBE_ID
re_legacy_video_key_name = re.compile(r"[\w-]+")

def get_or_create_unconverted_source_url(youtube_id):
    matching_keys = list(unconverted_bucket.list(youtube_id))
    matching_key = None

    if len(matching_keys) > 0:
        if len(matching_keys) > 1:
            logger.warning("More than 1 matching unconverted video URL found for video {0}".format(youtube_id))
        matching_key = matching_keys[0]
    else:
        logger.info("Unconverted video not available on s3 yet, downloading from youtube to create it.")

        video_path = youtube.download(youtube_id)
        logger.info("Downloaded video to {0}".format(video_path))

        assert(video_path)

        video_extension = splitext(video_path)
        assert video_extension[0] == "."
        video_extension = video_extension[1:]
        if video_extension not in ["flv", "mp4"]:
            logger.warning("Unrecognized video extension {0} when downloading video {1} from YouTube".format(video_extension, youtube_id))

        matching_key = Key(unconverted_bucket)
        matching_key.key = "{0}/{0}.{1}".format(youtube_id, video_extension)
        matching_key.set_contents_from_filename(video_path)

        os.remove(video_path)
        logger.info("Deleted {0}".format(video_path))

    return "s3://{0}/{1}".format(unconverted_bucket.name, matching_key.name)

def upload_unconverted_to_s3(youtube_id, video_path):

    s3_url = "s3://KA-youtube-unconverted/%s/%s" % (youtube_id, os.path.basename(video_path))

    command_args = ["s3cmd/s3cmd", "-c", "secrets/s3.s3cfg", "--acl-public", "put", video_path, s3_url]
    results = popen_results(command_args)
    logger.info(results)

    return s3_url

def list_converted_videos():
    """Returns a dict that maps youtube_ids (keys) to a set of available converted formats (values)"""
    converted_videos = defaultdict(set)
    legacy_video_keys = set()
    for key in converted_bucket.list(delimiter="/"):
        video_match = re_video_key_name.match(key.name)
        if video_match is None:
            if re_legacy_video_key_name.match(key.name) is not None:
                legacy_video_keys.add(key.name)
            else:
                logger.warning("Unrecognized key {0} is not in format YOUTUBE_ID.FORMAT/".format(key.name))
        else:
            converted_videos[video_match.group(1)].add(video_match.group(2))
    logger.info("{0} legacy converted videos were ignored".format(len(legacy_video_keys)))
    return converted_videos

def clean_up_video_on_s3(youtube_id):

    s3_unconverted_url = "s3://KA-youtube-unconverted/%s/" % youtube_id
    s3_converted_url = "s3://KA-youtube-converted/%s/" % youtube_id

    command_args = ["s3cmd/s3cmd", "-c", "secrets/s3.s3cfg", "--recursive", "del", s3_unconverted_url]
    results = popen_results(command_args)
    logger.info(results)

    command_args = ["s3cmd/s3cmd", "-c", "secrets/s3.s3cfg", "--recursive", "del", s3_converted_url]
    results = popen_results(command_args)
    logger.info(results)

def download_converted_from_s3(youtube_id):

    s3_folder_url = "s3://KA-youtube-converted/%s/" % youtube_id

    temp_dir = tempfile.gettempdir()
    video_folder_path = os.path.join(temp_dir, youtube_id)

    if os.path.exists(video_folder_path):
        shutil.rmtree(video_folder_path)

    os.mkdir(video_folder_path)

    command_args = ["s3cmd/s3cmd", "-c", "secrets/s3.s3cfg", "--recursive", "get", s3_folder_url, video_folder_path]
    results = popen_results(command_args)
    logger.info(results)

    return video_folder_path

def upload_converted_to_archive(video):

    youtube_id = video["youtube_id"]

    video_folder_path = download_converted_from_s3(youtube_id)
    assert(video_folder_path)
    assert(len(os.listdir(video_folder_path)))
    logger.info("Downloaded youtube id %s from s3 for archive export" % youtube_id)

    archive_bucket_url = "s3://KA-converted-%s" % youtube_id

    # Only pass ascii title and descriptions in headers to archive
    ascii_title = unicodedata.normalize("NFKD", video["title"] or u"").encode("ascii", "ignore")
    ascii_description = unicodedata.normalize("NFKD", video["description"] or u"").encode("ascii", "ignore")

    # Newlines not welcome in headers
    ascii_title = ascii_title.replace("\n", " ")
    ascii_description = ascii_description.replace("\n", " ")

    command_args = [
            "s3cmd/s3cmd", 
            "-c", "secrets/archive.s3cfg", 
            "--recursive", 
            "--force", 
            "--add-header", "x-archive-auto-make-bucket:1",
            "--add-header", "x-archive-meta-collection:khanacademy", 
            "--add-header", "x-archive-meta-title:%s" % ascii_title,
            "--add-header", "x-archive-meta-description:%s" % ascii_description,
            "--add-header", "x-archive-meta-mediatype:movies", 
            "--add-header", "x-archive-meta01-subject:Salman Khan", 
            "--add-header", "x-archive-meta02-subject:Khan Academy", 
            "put", video_folder_path + "/", archive_bucket_url]
    results = popen_results(command_args)
    logger.info(results)

    logger.info("Waiting 10 seconds")
    time.sleep(10)

    shutil.rmtree(video_folder_path)
    logger.info("Cleaned up local video folder path")

    return verify_archive_upload(youtube_id)

def verify_archive_upload(youtube_id):

    c_retries_allowed = 3
    c_retries = 0

    while c_retries < c_retries_allowed:
        try:
            request = urllib2.Request("http://s3.us.archive.org/KA-converted-%s/%s.mp4" % (youtube_id, youtube_id))

            request.get_method = lambda: "HEAD"
            response = urllib2.urlopen(request)

            return response.code == 200
        except urllib2.HTTPError, e:
            c_retries += 1

            if c_retries < c_retries_allowed:
                logger.error("Error during archive upload verification attempt %s, trying again" % c_retries)
            else:
                logger.error("Error during archive upload verification final attempt: %s" % e)

            time.sleep(5)

    return False
