import datetime
import logging
import optparse
import os
import time

import api
import youtube
import s3
import zencode
import filelock

# This script will make sure we have downloadable content for each video
# in the following formats.
DOWNLOADABLE_FORMATS = ["mp4", "png"] # TODO: remove png?

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

        logging.info("Searching for videos that are missing downloadable content")
        videos_converted = 0

        for video in api.list_videos():

            if videos_converted > max_videos:
                break

            formats_to_create = []

            # Build a list of to-be-created formats for this video
            for downloadable_format in DOWNLOADABLE_FORMATS:
                if downloadable_format not in video["downloadable_formats"]:
                    formats_to_create.append(downloadable_format)

            # Don't recreate any formats that are already waiting on s3
            # but are, for any reason, not known by the API.
            for format_already_created in s3.list_available_formats(video):
                formats_to_create.remove(format_already_created)

            if len(formats_to_create) > 0:

                logging.info("Starting conversion of %s into formats %s" % (video["youtube_id"], formats_to_create))

                s3_source_url = s3.get_or_create_unconverted_source_url(video)
                assert(s3_source_url)

                if dryrun:
                    logging.info("Skipping sending job to zencoder due to dryrun")
                else:
                    s3_urls_converting = zencode.start_converting(video, s3_source_url, thumbnail_time, formats_to_create)
                    assert(s3_urls_converting)

                logging.info("Started converting %s to %s" % (s3_source_url, s3_urls_converting))

                videos_converted += 1

    # Publish, to archive.org, all videos that have been converted to downloadable format
    @staticmethod
    def publish_converted_videos(max_videos, dryrun=False):

        logging.info("Searching for downloadable content that needs to be published to archive.org")

        # Get a list of all videos that are missing at least one downloadable format
        missing_content = api.list_missing_video_content(downloadable_formats)

        c_publish_attempts = 0

        for youtube_id in missing_content:

            if c_publish_attempts >= max_videos:
                break

            video = missing_content[youtube_id]
            available_formats = s3.list_available_formats(youtube_id)

            # Publish to archive if we've got new content waiting
            if len(available_formats) > len(video.downloadable_formats):

                if dryrun:
                    logging.info("Skipping upload to archive.org due to dryrun")

                else:
                    if s3.upload_available_formats_to_archive(video):
                        logging.info("Successfully uploaded to archive.org")

                        try:
                            if api.update_download_available(youtube_id, available_formats):
                                logging.info("Updated KA download_available, set to %s" % available_formats)
                            else:
                                logging.error("Unable to update KA download_available for youtube id %s" % youtube_id)
                        except Exception, e:
                            logging.error("Crash during update_download_available: %s" % e)
                            return

                    else:
                        logging.error("Unable to upload to archive.org")

                c_publish_attempts += 1

        logging.info("Done publishing.")

def setup_logging(options):

    try:
        os.mkdir("logs")
    except:
        pass

    strftime = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    if options.nolog:
        logging.basicConfig(
            level = logging.DEBUG,
            format='%(relativeCreated)dms %(message)s',
        )
    else:
        logging.basicConfig(
            level = logging.DEBUG,
            format='%(relativeCreated)dms %(message)s',
            filename = 'logs/%s_%s.log' % (options.step, strftime),
            filemode = 'w'
        )

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
        help="Don't start new zencoder jobs or upload to archive.org", default=1)

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
