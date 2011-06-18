import logging
import tempfile
import os
from util import popen_results
import urllib2
import json
import re

re_time = re.compile(r"(?P<hour>\d+):(?P<min>\d+):(?P<sec>\d+)(\.(?P<frac>\d+))?")

def parse_time(t):
    m = re_time.match(t)
    if m is None: return None
    secs = float(int(m.group("hour")) * 60 * 60 + int(m.group("min")) * 60 + int(m.group("sec")))
    if m.group("frac") is not None:
        secs += float("0.%s" % (m.group("frac"),))
    return secs

def download(video):

    temp_dir = tempfile.mkdtemp()

    youtube_id = video["youtube_id"]
    youtube_url = video["url"]

    thumbnail_time = None

    info_url = "http://gdata.youtube.com/feeds/api/videos/%s?alt=json" % (youtube_id,)
    info_stream = urllib2.urlopen(info_url)
    info = json.load(info_stream)

    thumbnails = info["entry"]["media$group"]["media$thumbnail"]
    for t in thumbnails:
        if "2.jpg" in t["url"]:
            thumbnail_time = parse_time(t["time"])
            break
    assert thumbnail_time is not None

    logging.info("Thumbnail time is %s", thumbnail_time)

    video_filename_template = youtube_id + ".%(ext)s"
    video_path_template = os.path.join(temp_dir, video_filename_template)

    command_args = ["python", "youtube-dl/youtube-dl.py", "--max-quality", "22", "-icw", "-o", video_path_template, youtube_url]
    results = popen_results(command_args)
    logging.info(results)

    files = os.listdir(temp_dir)
    assert len(files) == 1
    video_path = os.path.join(temp_dir, files[0])
    logging.info(video_path)

    return (youtube_id, video_path, thumbnail_time)
