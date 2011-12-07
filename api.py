import logging
import urllib2
import simplejson

from oauth import OAuthConsumer, OAuthToken, OAuthRequest, OAuthSignatureMethod_HMAC_SHA1
from secrets import ka_consumer_key, ka_consumer_secret, ka_access_token, ka_access_token_secret

def list_missing_video_content(downloadable_formats):

    file = urllib2.urlopen("http://www.khanacademy.org/api/v1/playlists/library/list/fresh")

    try:
        library = simplejson.loads(file.read())
    finally:
        file.close()

    missing_content = {}

    for playlist in library:
        for video in playlist["videos"]:
            for downloadable_format in downloadable_formats:
                if downloadable_format not in video.downloadable_formats:
                    # This video is missing some piece of downloadable content.
                    missing_content[video.youtube_id] = video

    return missing_content

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
    logging.info(response.read())

    return response.code == 200
