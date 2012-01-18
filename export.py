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

class YouTubeExporter(object):
    """ Convert our YouTube videos into downloadable formats.
    
    1) Take a YouTube URL and download the video to s3.
    2) Pass it through Zencoder to convert the video into various formats.
    3) Zencoder places the converted content in a different spot on s3.
    4) Upload from s3 to archive.org for their (very kind) free hosting.

    """

    @staticmethod
    def convert_missing_downloads(max_videos, dryrun=False, missing_on_s3=False):
        """ Download from YouTube and use Zencoder to start converting any missing downloadable content into
        its appropriate downloadable format.
        """

        videos_converted = 0

        if missing_on_s3:
            # With this option, videos that are missing in the S3 converted 
            # bucket are converted. The API's download_urls is ignored.
            logger.info("Searching for videos that are missing from S3")
            formats_to_convert = s3.list_missing_converted_formats()
            legacy_mp4_videos = s3.list_legacy_mp4_videos()
        else:
            # With this option (the default), videos that are missing in the API's
            # download_urls are converted, if they do not already exist on S3.
            # Videos that are missing from S3, but present in the API's 
            # download_urls, are ignored.
            logger.info("Searching for videos that are missing from API download_urls")
            formats_to_convert = api.list_missing_video_content()
            converted_formats = s3.list_converted_formats()

        for youtube_id, missing_formats in formats_to_convert.iteritems():
            if videos_converted >= max_videos:
                logger.info("Stopping: max videos reached")
                break

            if missing_on_s3:
                # We already know the formats are missing from S3.
                formats_to_create = missing_formats
                if youtube_id in legacy_mp4_videos and "mp4" in formats_to_create:
                    if dryrun:
                        logger.info("Skipping copy of legacy content due to dryrun")
                    else:
                        s3.copy_legacy_content_to_new_location(youtube_id)
                    formats_to_create.remove("mp4")
            else:
                # Don't recreate any formats that are already waiting on s3
                # but are, for any reason, not known by the API.
                already_converted_still_unpublished = converted_formats[youtube_id] & missing_formats
                if len(already_converted_still_unpublished) > 0:
                    logger.info("Video %s missing formats %s in API but they are already converted; use publish step" % (youtube_id, ",".join(already_converted_still_unpublished)))
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
    def publish_converted_videos(max_videos, dryrun=False, use_archive=True):

        logger.info("Searching for downloadable content that needs to be published")

        publish_attempts = 0
        converted_formats = s3.list_converted_formats()

        for youtube_id, missing_formats in api.list_missing_video_content().iteritems():
            if publish_attempts >= max_videos:
                logger.info("Stopping: max videos reached")
                break

            converted_missing_formats = converted_formats[youtube_id] & missing_formats

            unconverted_formats = missing_formats - converted_missing_formats
            if len(unconverted_formats) > 0:
                logger.info("Video %s missing formats %s which are still unconverted, can't be published" % (youtube_id, ",".join(unconverted_formats)))

            # If no converted content waiting, just continue to next video
            if len(converted_missing_formats) == 0:
                continue

            if dryrun:
                logger.info("Skipping publish for video {0} formats {1} due to dryrun".format(youtube_id, ", ".join(converted_missing_formats)))
            else:
                if use_archive:
                    if s3.upload_converted_to_archive(youtube_id, converted_missing_formats):
                        logger.info("Successfully uploaded to archive.org")
                    else:
                        logger.error("Unable to upload video {0} to archive.org".format(youtube_id))
                        continue
                else:
                    logger.info("Skipping upload to archive.org; assuming API points directly to S3 instead.")

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

    parser.add_option("--no-archive",
        action="store_false", dest="use_archive", default=True, 
        help="Assume the server uses S3 URLs instead of archive.org URLs. Don't upload to archive.org, and mark as published via API as soon as videos are converted and stored on S3.")

    parser.add_option("--missing-on-s3",
        action="store_true", dest="missing_on_s3", default=False,
        help="Convert any videos that are missing on S3 (as opposed to missing in the API download_urls)")

    options, args = parser.parse_args()

    setup_logging(options)

    # Grab a lock that times out after 2 days
    with filelock.FileLock("export.lock", timeout=2):
        if options.step == "convert":
            YouTubeExporter.convert_missing_downloads(options.max, options.dryrun, options.missing_on_s3)
        elif options.step == "publish":
            YouTubeExporter.publish_converted_videos(options.max, options.dryrun, options.use_archive)
        else:
            print "Unknown export step."

if __name__ == "__main__":
    main()
