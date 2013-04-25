import argparse
import errno
import json
import logging
import os
import sys
import time
import urllib2

from known_channels import known_language_channels


def _channel_json_filename(channel_id):
    return os.path.join(os.path.dirname(__file__), 'youtube_data',
            '%s.json' % channel_id)


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

    filename = _channel_json_filename(channel_id)
    logging.info("Writing data to %s." % filename)
    with open(filename, 'wb') as fp:
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
        response = make_request(url)
        entry = response["feed"].get("entry")
        if entry is None:
            more_videos = False
        else:
            start_index += 50
            video_entries += entry
    return video_entries


def playlist_ids(channel_id):
    """Return the combined JSON response of all the channel's playlists."""
    logging.info("Getting all playlists for %s" % channel_id)
    url = "https://gdata.youtube.com/feeds/api/users/%s/playlists?v=2&alt=json&max-results=50" % channel_id
    response = make_request(url)
    playlists = response["feed"].get("entry", [])
    return [playlist["yt$playlistId"]["$t"] for playlist in playlists]


def playlist_videos(playlist_id):
    """Return the combined JSON response of all the videos in the playlist."""
    logging.info("Getting all videos for playlist: %s" % playlist_id)
    playlist_entries = []
    start_index = 1
    more_videos = True
    while more_videos:
        url = "http://gdata.youtube.com/feeds/api/playlists/%s?v=2&alt=json&max-results=50&start-index=%d" % (playlist_id, start_index)
        response = make_request(url)
        entry = response["feed"].get("entry")
        if entry is None:
            more_videos = False
        else:
            start_index += 50
            playlist_entries += entry
    return playlist_entries


def make_request(url):
    """Make an API request and handle errors, retrying up to 5 times."""
    for n in range(5):
        time.sleep(1)
        try:
            request = urllib2.Request(url)
            response = json.load(urllib2.urlopen(request))
        except Exception, e:
            logging.error(
                "Error during request. Trying again %d/5 times.", n + 1)
            logging.debug("Error: %s.\nURL: %s", e, url)
            if n == 5:
                raise
        else:
            return response


def video_ids_set(channel_ids=None):
    """Return a set of all video IDs in the specified language channels.
    Return all video IDs if left empty.
    """
    if channel_ids:
        logging.info(
                "Returning set of video IDs for %s", ", ".join(channel_ids))
    else:
        logging.info("Returning set of all language channel video IDs")
        channel_ids = known_language_channels.keys()

    video_ids = set()
    for channel_id in channel_ids:
        if channel_id not in known_language_channels:
            raise KeyError("Unknown language channel %r" % channel_id)
        video_ids.update(extract_ids(channel_id))
    return video_ids


def extract_ids(channel_id):
    """Return a set of video IDs that have been uploaded or included in
    playlists of the channel.
    """
    video_ids = set()
    filename = _channel_json_filename(channel_id)
    try:
        with open(filename, 'r') as f:
            data = json.load(f)
    except IOError, e:
        if e.errno == errno.ENOENT:
            logging.warning("File %s not found, skipping", filename)
            return set()
        else:
            raise

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


def setup_logging():
    logging.basicConfig(level=logging.INFO,
            format='%(levelname)s\t%(asctime)s\t%(message)s')


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
            "-U", "--update", action="store_true", dest="update",
            help="Request updated data on language channels via the YouTube "
            "API.")

    parser.add_argument(
            "-l", "--language-channel", action="append",
            dest="language_channels", default=None,
            help="Languages to update. Ex: -l 'KhanAcademyRussian' "
            "-l 'KhanAcademyDansk'. Default is all.")

    parser.add_argument(
            "-q", "--quiet", action="store_true",
            help="Suppress log output.")

    parser.add_argument(
            "-v", "--videos", action="store_true", dest="videos_set",
            help="Print a list of video IDs for the specified "
            "language channels. If language channels are not "
            "specified it will return a set of all video ids for "
            "all language channels.")

    args = parser.parse_args()

    if not args.videos_set and not args.update:
        parser.print_help()
        return 1

    if not args.quiet:
        setup_logging()

    if args.update:
        channels = args.language_channels or known_language_channels
        for channel_id in channels:
            if channel_id not in known_language_channels:
                logging.error("Unknown language channel %r", channel_id)
                return 1

            try:
                update_language_channel_json(channel_id)
            except Exception:
                logging.exception(
                        "Unable to update JSON for channel %r", channel_id)

    if args.videos_set:
        for video_id in video_ids_set(args.language_channels):
            print video_id

    return 0


if __name__ == '__main__':
    sys.exit(main())
