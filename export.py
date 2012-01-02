import datetime
import logging
import optparse
import os
import time

import api
import s3
import zencode
import filelock
from util import logger

# This script will make sure we have downloadable content for each video
# in the following formats.
DOWNLOADABLE_FORMATS = set(["mp4", "m3u8"])

class YouTubeExporter(object):
    """ Convert our YouTube videos into downloadable formats.
    
    1) Take a YouTube URL and download the video to s3.
    2) Pass it through Zencoder to convert the video into various formats.
    3) Zencoder places the converted content in a different spot on s3.
    4) Upload from s3 to archive.org for their (very kind) free hosting.

    """

    @staticmethod
    def convert_missing_downloads(max_videos, dryrun=False):
        """ Download from YouTube and use Zencoder to start converting any missing downloadable content into
        its appropriate downloadable format.
        """

        logger.info("Searching for videos that are missing downloadable content")
        videos_converted = 0
        converted_videos = s3.list_converted_videos()

        for youtube_id, missing_formats in api.list_missing_video_content(DOWNLOADABLE_FORMATS).iteritems():
            if videos_converted >= max_videos:
                logger.info("Stopping: max videos reached")
                break

            # Don't recreate any formats that are already waiting on s3
            # but are, for any reason, not known by the API.
            already_converted_still_unpublished = converted_videos[youtube_id] & missing_formats
            if len(already_converted_still_unpublished) > 0:
                logger.info("Video %s missing formats %s which are already converted, but unpublished; use publish step" % (youtube_id, ",".join(already_converted_still_unpublished)))

            formats_to_create = missing_formats - already_converted_still_unpublished
            if len(formats_to_create) == 0:
                continue

            logger.info("Starting conversion of %s into formats %s" % (youtube_id, ",".join(formats_to_create)))

            s3_source_url = s3.get_or_create_unconverted_source_url(youtube_id)
            assert(s3_source_url)

            if dryrun:
                logger.info("Skipping sending job to zencoder due to dryrun")
            else:
                zencode.start_converting(youtube_id, s3_source_url, formats_to_create)

            videos_converted += 1

    # Publish, to archive.org, all videos that have been converted to downloadable format
    @staticmethod
    def publish_converted_videos(max_videos, dryrun=False):

        logger.info("Searching for downloadable content that needs to be published to archive.org")

        publish_attempts = 0
        converted_videos = s3.list_converted_videos()

        for youtube_id, missing_formats in api.list_missing_video_content(DOWNLOADABLE_FORMATS).iteritems():
            if publish_attempts >= max_videos:
                logger.info("Stopping: max videos reached")
                break

            converted_missing_formats = converted_videos[youtube_id] & missing_formats

            unconverted_formats = missing_formats - converted_missing_formats
            if len(unconverted_formats) > 0:
                logger.info("Video %s missing formats %s which are still unconverted, can't be published" % (youtube_id, ",".join(unconverted_formats)))

            # If no converted content waiting, just continue to next video
            if len(converted_missing_formats) == 0:
                continue

            if dryrun:
                logger.info("Skipping upload to archive.org for video {0} formats {1} due to dryrun".format(youtube_id, ", ".join(converted_missing_formats)))
            else:
                if s3.upload_converted_to_archive(youtube_id, converted_missing_formats):
                    logger.info("Successfully uploaded to archive.org")

                    current_format_downloads = (api.video_metadata(youtube_id)["download_urls"] or {})
                    current_formats = set(current_format_downloads.keys())
                    new_formats = current_formats | converted_missing_formats
                    if "mp4" in new_formats:
                        # PNG thumbnails are generated as part of the MP4 conversion process.
                        # If mp4 has been uploaded to archive.org, png is guaranteed to be there as well.
                        new_formats.add("png")
                    if api.update_download_available(youtube_id, new_formats):
                        logger.info("Updated KA download_available, set to {0} for video {1}".format(", ".join(new_formats), youtube_id))
                    else:
                        logger.error("Unable to update KA download_available to {0} for youtube id {1}".format(", ".join(new_formats), youtube_id))
                else:
                    logger.error("Unable to upload video {0} to archive.org".format(youtube_id))

            publish_attempts += 1


def setup_logging(options):
    formatter = logging.Formatter(fmt='%(relativeCreated)dms %(message)s')

    if options.nolog:
        handler = logging.StreamHandler()
    else:
        if not os.path.isdir("logs"):
            os.mkdir("logs")

        strftime = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        handler = logging.FileHandler('logs/%s_%s.log' % (options.step, strftime), mode="w")

    handler.setFormatter(formatter)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)

def main():

    parser = optparse.OptionParser()

    parser.add_option("-s", "--step",
        action="store", dest="step",
        help="Export step ('convert' or 'publish' currently)", default="convert")

    parser.add_option("-n", "--no-log",
        action="store_true", dest="nolog",
        help="Don't store log file", default=False)

    parser.add_option("-m", "--max",
        action="store", dest="max", type="int",
        help="Maximum number of videos to process", default=1)

    parser.add_option("-d", "--dryrun",
        action="store_true", dest="dryrun",
        help="Don't start new zencoder jobs or upload to archive.org", default=False)

    options, args = parser.parse_args()

    setup_logging(options)

    # Grab a lock that times out after 2 days
    with filelock.FileLock("export.lock", timeout=2):
        if options.step == "convert":
            YouTubeExporter.convert_missing_downloads(options.max, options.dryrun)
        elif options.step == "publish":
            YouTubeExporter.publish_converted_videos(options.max, options.dryrun)
        else:
            print "Unknown export step."

if __name__ == "__main__":
    main()
