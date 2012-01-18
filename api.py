import logging
import urllib2
import simplejson

from oauth import OAuthConsumer, OAuthToken, OAuthRequest, OAuthSignatureMethod_HMAC_SHA1
from secrets import ka_consumer_key, ka_consumer_secret, ka_access_token, ka_access_token_secret
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

def list_missing_video_content():
    """Returns a dictionary mapping youtube IDs to formats missing from the API"""

    missing_content = {}

    for playlist in get_library():
        for video in playlist["videos"]:
            download_urls = video["download_urls"]
            if download_urls is None:
                download_urls = {}
            missing_formats = DOWNLOADABLE_FORMATS - set(download_urls.keys())
            if len(missing_formats) > 0:
                missing_content[video["youtube_id"]] = missing_formats

    return missing_content

def video_metadata(youtube_id):
    """Returns metadata dict (title, description, etc.) for a given youtube_id."""
    for playlist in get_library():
        for video in playlist["videos"]:
            if video["youtube_id"] == youtube_id:
                return video

def update_download_available(youtube_id, available_formats):

    consumer = OAuthConsumer(ka_consumer_key, ka_consumer_secret)
    access_token = OAuthToken(ka_access_token, ka_access_token_secret)
    url = "http://www.khanacademy.org/api/v1/videos/%s/download_available" % youtube_id

    oauth_request = OAuthRequest.from_consumer_and_token(
            consumer,
            token = access_token,
            http_url = url,
            http_method="POST",
            parameters={"formats": ",".join(available_formats)},
            )

    oauth_request.sign_request(OAuthSignatureMethod_HMAC_SHA1(), consumer, access_token)

    response = urllib2.urlopen(oauth_request.get_normalized_http_url(), data=oauth_request.to_postdata())
    logger.info(response.read())

    return response.code == 200
