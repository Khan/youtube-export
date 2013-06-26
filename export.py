import datetime
import logging
import optparse
import os

import s3
import zencode
import filelock
from util import logger


class YouTubeExporter(object):
    """ Convert our YouTube videos into downloadable formats.

    1) Take a YouTube URL and download the video to s3.
    2) Pass it through Zencoder to convert the video into various formats.
    3) Zencoder places the converted content in a different spot on s3.

    """

    @staticmethod
    def convert_missing_downloads(max_videos, dryrun=False):
        """Download from YouTube and use Zencoder to start converting any
        missing downloadable content into its appropriate downloadable format.
        """

        videos_converted = 0

        # With this option, videos that are missing in the S3 converted
        # bucket are converted. The API's download_urls is ignored.
        logger.info("Searching for videos that are missing from S3")
        formats_to_convert = s3.list_missing_converted_formats()
        legacy_mp4_videos = s3.list_legacy_mp4_videos()

        for youtube_id, missing_formats in formats_to_convert.iteritems():
            if videos_converted >= max_videos:
                logger.info("Stopping: max videos reached")
                break

            if "_DUP_" in youtube_id:
                logger.info(
                    ("Skipping video {0} as it has invalid DUP in youtube ID"
                     .format(youtube_id)))
                continue

            # We already know the formats are missing from S3.
            formats_to_create = missing_formats
            if (youtube_id in legacy_mp4_videos and
                    "mp4" in formats_to_create):
                if dryrun:
                    logger.info(
                        "Skipping copy of legacy content due to dryrun")
                else:
                    s3.copy_legacy_content_to_new_location(youtube_id)
                formats_to_create.remove("mp4")

            if len(formats_to_create) == 0:
                continue

            logger.info("Starting conversion of %s into formats %s" %
                        (youtube_id, ",".join(formats_to_create)))

            if dryrun:
                logger.info(
                    "Skipping downloading and sending job to zencoder due to "
                    "dryrun")
            else:
                s3_source_url = s3.get_or_create_unconverted_source_url(
                    youtube_id)
                assert(s3_source_url)

                zencode.start_converting(
                    youtube_id, s3_source_url, formats_to_create)

            videos_converted += 1


def setup_logging(options):
    formatter = logging.Formatter(fmt='%(relativeCreated)dms %(message)s')

    if options.nolog:
        handler = logging.StreamHandler()
    else:
        if not os.path.isdir("logs"):
            os.mkdir("logs")

        strftime = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        handler = logging.FileHandler(
            'logs/convert_%s.log' % strftime, mode="w")

    handler.setFormatter(formatter)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)


def main():
    parser = optparse.OptionParser()

    parser.add_option("-n", "--no-log",
        action="store_true", dest="nolog",
        help="Don't store log file", default=False)

    parser.add_option("-m", "--max",
        action="store", dest="max", type="int",
        help="Maximum number of videos to process", default=1)

    parser.add_option("-d", "--dryrun",
        action="store_true", dest="dryrun",
        help="Don't start new zencoder jobs or upload to s3",
        default=False)

    options, args = parser.parse_args()

    setup_logging(options)

    # Grab a lock that times out after 2 days
    with filelock.FileLock("export.lock", timeout=2):
        YouTubeExporter.convert_missing_downloads(
            options.max, options.dryrun)

if __name__ == "__main__":
    main()
