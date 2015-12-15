"""Encode all videos under a single topic with aggressive compression settings.

This is for testing out and tweaking compression settings.
"""
import argparse
import contextlib
import json
import urllib2

import zencode


def get_arguments():
    parser = argparse.ArgumentParser(
            description="Aggressively compress videos under a given topic")

    parser.add_argument('topic_slug',
            help="Slug of topic to encode videos for")

    parser.add_argument("--base-url",
            default="https://s3.amazonaws.com/ka-david-test-bucket/",
            help="Base S3 URL for output")

    parser.add_argument("--dry-run", action="store_true", default=False,
            help="Don't start Zencoder jobs; just print videos to be encoded")

    return parser.parse_args()


def get_youtube_ids(topic_slug):
    """Get a list of all youtube IDs for a single topic."""
    # We get the entire topictree instead of using the /topic/<topic>/videos
    # endpoint because that one doesn't seem to work for non-terminal topics
    # (e.g. subjects).
    api_url = "https://www.khanacademy.org/api/v1/topictree?kind=Video"
    with contextlib.closing(urllib2.urlopen(api_url)) as topictree_request:
        topictree_root = json.load(topictree_request)

    youtube_ids = []

    def _traverse_tree(node, seen_our_topic_slug):
        if node["kind"] == "Video" and seen_our_topic_slug:
            youtube_ids.append(node["youtube_id"])
        elif node["kind"] == "Topic":
            seen_our_topic_slug |= (node["slug"] == topic_slug)
            for child in node["children"]:
                _traverse_tree(child, seen_our_topic_slug)

    _traverse_tree(topictree_root, False)
    return youtube_ids


def main():
    args = get_arguments()

    print "Fetching video YouTube IDs under topic %s" % args.topic_slug
    youtube_ids = get_youtube_ids(args.topic_slug)

    for youtube_id in youtube_ids:
        source_url = "s3://KA-youtube-converted/%s.mp4/%s.mp4" % (
                youtube_id, youtube_id)
        print "Converting YouTube video %s on Zencoder (source url: %s)" % (
                youtube_id, source_url)
        if not args.dry_run:
            zencode.start_converting(youtube_id, source_url,
                    ["mp4", "m3u8"], base_url=args.base_url)

    print
    print "See %s running jobs at https://app.zencoder.com/jobs" % len(
            youtube_ids)


if __name__ == '__main__':
    main()
