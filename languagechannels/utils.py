import optparse
import logging
import urllib2
import json
import pdb
import csv
import time
from known_channels import known_language_channels
import os


def update_all_language_channels_json():
    """Write a new JSON file for each language in the built-in KA language
    channel dictionary.
    """
    logging.info("Updating all language channels:")
    for channel_id, language_code in known_language_channels.iteritems():
        update_language_channel_json(channel_id)


def update_language_channel_json(channel_id):
    """Write a new JSON file for specified channel."""
    uploads_json = channel_uploads(channel_id)
    playlist_ids_list = playlist_ids(channel_id)
    playlist_videos_json = {}
    for pl_id in playlist_ids_list:
        playlist_videos_json[pl_id] = playlist_videos(pl_id)
    full_channel_json = {
        "video_uploads": uploads_json,
        "playlist_videos": playlist_videos_json
    }
    directory = os.path.dirname(os.path.abspath(__file__)) + '/youtube_data/'
    filename = '%s.json' % channel_id
    full_file_path = directory + filename
    if not os.path.exists(directory):
        os.makedirs(directory)
        logging.info("Created directory %s." % directory)
    logging.info("Writing data to %s." % full_file_path)
    with open(full_file_path, 'wb') as fp:
        json.dump(full_channel_json, fp)


def channel_uploads(channel_id):
    """Return the combined JSON response of all the channel's
    uploaded videos.
    """
    logging.info("Getting all uploads for %s" % channel_id)
    video_entries = []
    start_index = 1
    more_videos = True
    while more_videos:
        url = "https://gdata.youtube.com/feeds/api/users/%s/uploads?alt=json&max-results=50&start-index=%d" % (channel_id, start_index)
        try:
            response = make_request(url)
        except Exception, e:
            logging.info("Setting more_videos to false to continue execution.")
            more_videos = False
        else:
            entry = response.get("feed").get("entry")
            if entry == None:
                more_videos = False
            else:
                start_index += 50
                video_entries += entry
    return video_entries


def playlist_ids(channel_id):
    """Return the combined JSON response of all the channel's playlists."""
    logging.info("Getting all playlists for %s" % channel_id)
    playlist_ids = []
    url = "https://gdata.youtube.com/feeds/api/users/%s/playlists?v=2&alt=json&max-results=50" % channel_id
    try:
        response = make_request(url)
    except Exception, e:
        return playlist_ids
    else:
        entry = response.get("feed").get("entry")
        if entry == None:
            return playlist_ids
        else:
            for playlist_id in entry:
                playlist_ids.append(playlist_id["yt$playlistId"]["$t"])
            return playlist_ids


def playlist_videos(playlist_id):
    """Return the combined JSON response of all the videos in the playlist."""
    logging.info("Getting all videos for playlist: %s" % playlist_id)
    playlist_entries = []
    start_index = 1
    more_videos = True
    while more_videos:
        url = "http://gdata.youtube.com/feeds/api/playlists/%s?v=2&alt=json&max-results=50&start-index=%d" % (
            playlist_id, start_index)
        try:
            response = make_request(url)
        except Exception, e:
            logging.info("Setting more_videos to false to continue execution.")
            more_videos = False
        else:
            entry = response.get("feed").get("entry")
            if entry == None:
                more_videos = False
            else:
                start_index += 50
                playlist_entries += entry
    return playlist_entries


def make_request(url):
    """Make an API request and handle errors, retry up to 5 times."""
    for n in range(5):
        time.sleep(1)
        try:
            request = urllib2.Request(url)
            response = json.load(urllib2.urlopen(request))
        except Exception, e:
            logging.error(
                "Error during request. Trying again %d/5 times." % (n+1))
            logging.debug("Error: %s.\nURL: %s" % (e, url))
            if n == 5:
                raise Exception("Error during request.")
        else:
            return response


def video_ids_set(channel_ids=None):
    """Return a set of all video IDs in the specified language channels.
    Return all video IDs if left empty.
    """
    if channel_ids:
        logging.info("Returning set of video IDs for %s" %
                     ", ".join(channel_ids))
    else:
        logging.info("Returning set of all language channel video IDs")
        channel_ids = known_language_channels.keys()

    video_ids = set()
    for channel_id in channel_ids:
        try:
            ensure_existence(channel_id)
        except Exception, e:
            logging.error(
                "'%s' is not a language channel. Check for misspellings and try again! :)" % channel_id)
        else:
            video_ids.update(extract_ids(channel_id))
    return video_ids


def extract_ids(channel_id):
    """Return a set of video IDs that have been uploaded or included in
    playlists of the channel.
    """
    video_ids = set()
    data = json.load(open(os.path.dirname(os.path.realpath(
        __file__)) + '/youtube_data/%s.json' % channel_id))
    # Extract uploaded video IDs
    for entry in data["video_uploads"]:
        video_id = entry["id"]["$t"].replace(
            "http://gdata.youtube.com/feeds/api/videos/", "")
        assert len(video_id) == 11
        video_ids.add(video_id)
    # Extract playlist video IDs
    for playlist_id in data["playlist_videos"]:
        for video in data["playlist_videos"][playlist_id]:
            video_ids.add(video["media$group"]["yt$videoid"]["$t"])
    return video_ids


def ensure_existence(channel_id):
    """Ensure that the language channel ID given exists inside the language
    channel dictionary.
    """
    if channel_id not in known_language_channels:
        raise Exception(
            "'%s' is not a language channel. Check for misspellings and try again! :)" % channel_id)


def setup_logging():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')


def main():
    parser = optparse.OptionParser()

    parser.add_option("-U", "--update", action="store_true", dest="update",
                      help="Request updated data on language channels via the YouTube API.",
                      default=False)

    parser.add_option("-l", "--language-channel", action="append",
                      dest="language_channels", help="Languages to update. Ex: -l "
                      "'KhanAcademyRussian' -l 'KhanAcademyDansk'. Default is all.",
                      default=None)

    parser.add_option("-q", "--quiet", action="store_true", dest="quiet",
                      help="Suppress output.")

    parser.add_option("-v", "--videos", action="store_true", dest="videos_set",
                      help="Return a set of videos for the specified language channels. "
                      "If language channels are not specified it will return a set of all "
                      "video ids for all language channels.")

    options, args = parser.parse_args()

    if not options.quiet:
        setup_logging()

    if options.update:
        if not options.language_channels:
            update_all_language_channels_json()
        else:
            for language_ch in options.language_channels:
                if ensure_existence(language_ch):
                    update_language_channel_json(language_ch)

    if options.videos_set:
        return video_ids_set(options.language_channels)

    if not options.videos_set and not options.language_channels and not options.update:
        logging.info("Use --help to see runnable options.")


if __name__ == '__main__':
    main()
