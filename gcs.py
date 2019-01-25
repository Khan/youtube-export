"""Talking to Google Cloud Storage module.

We use this module to talking to GCS. To list converted formats;
and get the URL of converted youtube videos files.
"""
import api
import argparse
import collections
import datetime
import os
import pprint
import re
import sys
import util

import youtube
from google.cloud import storage

try:
    storage_client = storage.Client()
except google.cloud.DefaultCredentialsError:
    sys.exit("Did not detect GOOGLE_APPLICATION_CREDENTIALS")

converted_bucket = storage_client.bucket("ka-youtube-converted")
unconverted_bucket = storage_client.bucket("ka-youtube-unconverted")

# Keys (inside buckets) are in the format YOUTUBE_ID.FORMAT
# e.g. DK1lCc9b7bg.m3u8/ or Dpo_-GrMpNE.mp4-low/
re_video_key_name = re.compile(r"([\w-]+)\.([\w-]+)/")

# Older keys are of the form YOUTUBE_ID
re_legacy_video_key_name = re.compile(r"([\w-]+)/(.*)$")


def get_or_create_unconverted_source_url(youtube_id):
    matching_keys = list(unconverted_bucket.list_blobs(prefix=youtube_id))

    # TODO(alpert): How do these .part files get created? They're not real
    # video files and should be ignored.
    matching_keys = [key for key in matching_keys
                     if not key.name.endswith('.part')]

    matching_key = None

    if matching_keys:
        if len(matching_keys) > 1:
            util.logger.warning("More than 1 matching unconverted video "
                                "URL found for video {0}".format(youtube_id))
        matching_key = matching_keys[0]
    else:
        util.logger.info("Unconverted video not available on GCS yet, "
                         "downloading from youtube to create it.")

        video_path = youtube.download(youtube_id)
        if not video_path:
            message = "Error downloading video {0}".format(youtube_id)
            util.logger.warning(message)
            return
        util.logger.info("Downloaded video to {0}".format(video_path))

        video_extension = os.path.splitext(video_path)[1]
        assert video_extension[0] == "."
        video_extension = video_extension[1:]
        if video_extension not in ["flv", "mp4"]:
            message = ("Unrecognized video extension {0} when downloading "
                       "video {1} from YouTube".format(video_extension,
                                                       youtube_id))
            util.logger.warning(message)
        # Upload video file into gcs unconverted bucket.
        matching_key = unconverted_bucket.blob(
            "{0}/{0}.{1}".format(youtube_id,
                                 video_extension))
        matching_key.upload_from_filename(video_path)

        os.remove(video_path)
        util.logger.info("Deleted {0}".format(video_path))

    return "gcs://{0}/{1}".format(unconverted_bucket.name, matching_key.name)


# In order to get all "prefixes" or "dirs" in GCS, We manually use a
# private method on the iterator to work around missing API functionality.
# See https://github.com/googleapis/google-cloud-python/issues/920
def list_gcs_directories(bucket, prefix=None):
    iterator = bucket.list_blobs(prefix=prefix, delimiter='/')
    prefixes = set()
    #  In order to fetch *all* the prefixes, need to loop all pages
    for page in iterator.pages:
        prefixes.update(page.prefixes)
    return prefixes


def list_converted_formats():
    """Get map of youtube_ids (keys) to set of available converted formats."""
    converted_videos = collections.defaultdict(set)
    legacy_video_keys = set()
    dirs = list_gcs_directories(converted_bucket)
    for key in dirs:
        video_match = re_video_key_name.match(key)
        if video_match is None:
            if re_legacy_video_key_name.match(key) is not None:
                legacy_video_keys.add(key)
            else:
                util.logger.warning("Unrecognized key {0} is not in format "
                                    "YOUTUBE_ID.FORMAT/".format(key))
        else:
            converted_videos[video_match.group(1)].add(video_match.group(2))
    util.logger.info("{0} legacy converted videos were ignored".format(
        len(legacy_video_keys)))
    return converted_videos


def list_legacy_mp4_videos():
    """Return a set of legacy converted youtube_ids.

    Returns youtube ids of videos that have legacy mp4/png converted
    content saved on GCS. You can pass these ids to
    copy_legacy_content_to_new_location.
    """
    legacy_mp4_videos = set()
    dirs = list_gcs_directories(converted_bucket)
    for key in dirs:
        legacy_match = re_legacy_video_key_name.match(key)
        if legacy_match is not None:
            legacy_mp4_videos.add(legacy_match.group(1))
    return legacy_mp4_videos


def copy_legacy_content_to_new_location(youtube_id):
    """Copy MP4 & PNG files from a legacy-format video to new naming scheme.
    """
    blobs = converted_bucket.list_blobs(prefix="{0}/".format(youtube_id))
    for key in blobs:
        legacy_match = re_legacy_video_key_name.match(key.name)
        assert legacy_match is not None
        assert legacy_match.group(1) == youtube_id
        dest_key = "{0}.mp4/{1}".format(youtube_id, legacy_match.group(2))
        util.logger.info("Copying {0} to {1}".format(key.name, dest_key))

        new_blob = converted_bucket.copy_blob(key, converted_bucket,
                                              dest_key, preserve_acl=True)


def list_missing_converted_formats():
    """Return a map of youtube_ids to a set of missing formats on GCS."""
    missing_converted_formats = {}
    converted_formats = list_converted_formats()
    for youtube_id in api.get_youtube_ids():
        missing_converted_formats[youtube_id] = (
            util.DOWNLOADABLE_FORMATS - converted_formats[youtube_id])
    return missing_converted_formats
