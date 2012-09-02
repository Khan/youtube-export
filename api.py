import urllib
import urllib2
import simplejson

import secrets
from util import logger, DOWNLOADABLE_FORMATS

_library = None


def get_library():
    global _library
    if _library is None:
        fresh = urllib2.urlopen("http://www.khanacademy.org/api/v1/playlists/library/list/fresh")
        try:
            _library = simplejson.load(fresh)
        finally:
            fresh.close()
    return _library


def list_videos():
    for playlist in get_library():
        for video in playlist["videos"]:
            if video["kind"] != "Video":
                continue
            yield video


def list_missing_video_content():
    """Return a dict mapping youtube IDs to formats missing from the API."""

    missing_content = {}

    for video in list_videos():
        download_urls = video["download_urls"]
        if download_urls is None:
            download_urls = {}
        missing_formats = DOWNLOADABLE_FORMATS - set(download_urls.keys())
        if len(missing_formats) > 0:
            missing_content[video["youtube_id"]] = missing_formats

    return missing_content


def video_metadata(youtube_id):
    """Returns metadata dict (title, description, etc.) for a given
    youtube_id."""
    for video in list_videos():
        if video["youtube_id"] == youtube_id:
            return video


def update_download_available(youtube_id, available_formats):

    url = "http://www.khanacademy.org/api/v1/videos/%s/download_available" % youtube_id
    params = {
        'formats': ','.join(available_formats),
        'key': secrets.ka_download_available_secret,
    }

    response = urllib2.urlopen(url, data=urllib.urlencode(params))
    logger.info(response.read())

    return response.code == 200
