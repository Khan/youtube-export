import logging
import tempfile
import os
import urllib2
import json
import re

from util import popen_results, logger

re_time = re.compile(r"(?P<hour>\d+):(?P<min>\d+):(?P<sec>\d+)(\.(?P<frac>\d+))?")

def parse_time(t):
    m = re_time.match(t)
    if m is None: return None
    secs = float(int(m.group("hour")) * 60 * 60 + int(m.group("min")) * 60 + int(m.group("sec")))
    if m.group("frac") is not None:
        secs += float("0.%s" % (m.group("frac"),))
    return secs

def get_thumbnail_time(youtube_id):
    thumbnail_time = None

    info_url = "http://gdata.youtube.com/feeds/api/videos/%s?alt=json" % (youtube_id,)
    info_stream = urllib2.urlopen(info_url)
    info = json.load(info_stream)

    thumbnails = info["entry"]["media$group"]["media$thumbnail"]
    for t in thumbnails:
        if "2.jpg" in t["url"]:
            thumbnail_time = parse_time(t["time"])
            break
    if thumbnail_time is None:
        raise ValueError("No thumbnail time found in %s" % thumbnails)

    return thumbnail_time

def download(youtube_id):
    temp_dir = tempfile.mkdtemp()

    # Fake up a YouTube URL since youtube-dl expects one
    youtube_url = "http://www.youtube.com/watch?v={0}".format(youtube_id)

    video_filename_template = youtube_id + ".%(ext)s"
    video_path_template = os.path.join(temp_dir, video_filename_template)

    command_args = ["python", "youtube-dl/youtube-dl.py", "--max-quality", "22", "-icw", "-o", video_path_template, youtube_url]
    results = popen_results(command_args)
    logger.info(results)

    files = os.listdir(temp_dir)
    if not files:
        return
    assert len(files) == 1
    video_path = os.path.join(temp_dir, files[0])
    logger.info(video_path)

    return video_path
