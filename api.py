import logging
import urllib2
import simplejson

from oauth import OAuthConsumer, OAuthToken, OAuthRequest, OAuthSignatureMethod_HMAC_SHA1
from secrets import ka_consumer_key, ka_consumer_secret, ka_access_token, ka_access_token_secret

def list_new_videos():

    file = urllib2.urlopen("http://www.khanacademy.org/api/v1/playlists/library/list")

    try:
        library = simplejson.loads(file.read())
    finally:
        file.close()

    videos_new = []

    for playlist in library:
        for video in playlist["videos"]:
            if not video["download_urls"]:
                videos_new.append(video)

    return videos_new

def update_download_available(youtube_id):

    consumer = OAuthConsumer(ka_consumer_key, ka_consumer_secret)
    access_token = OAuthToken(ka_access_token, ka_access_token_secret)
    url = "http://www.khanacademy.org/api/v1/videos/%s/download_available" % youtube_id

    oauth_request = OAuthRequest.from_consumer_and_token(
            consumer,
            token = access_token,
            http_url = url,
            http_method="POST",
            parameters={"available": "1"},
            )

    oauth_request.sign_request(OAuthSignatureMethod_HMAC_SHA1(), consumer, access_token)

    response = urllib2.urlopen(oauth_request.get_normalized_http_url(), data=oauth_request.to_postdata())
    logging.info(response.read())

    return response.code == 200
