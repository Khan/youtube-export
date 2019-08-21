#!/usr/bin/env python
# TODO(colin): fix these lint errors (http://pep8.readthedocs.io/en/release-1.7.x/intro.html#error-codes)
# pep8-disable:E128

import optparse
import sys

import gcs
import zencode
import filelock
import util

logger = util.logger


class YouTubeExporter(object):
    """ Convert our YouTube videos into downloadable formats.

    1) Take a YouTube URL and download the video to gcs.
    2) Pass it through Zencoder to convert the video into various formats.
    3) Zencoder places the converted content in a different spot on gcs.

    """

    @staticmethod
    def convert_missing_downloads(max_videos, dryrun=False):
        """Download from YouTube and use Zencoder to start converting any
        missing downloadable content into its appropriate downloadable format.
        """

        videos_converted = 0
        error_ids = []

        # With this option, videos that are missing in the gcs converted
        # bucket are converted. The API's download_urls is ignored.
        logger.info("Searching for videos that are missing from gcs")
        formats_to_convert = gcs.list_missing_converted_formats()
        legacy_mp4_videos = gcs.list_legacy_mp4_videos()

        for youtube_id, missing_formats in formats_to_convert.iteritems():
            if videos_converted >= max_videos:
                logger.info("Stopping: max videos reached")
                break

            if "_DUP_" in youtube_id:
                logger.info(
                    ("Skipping video {0} as it has invalid DUP in youtube ID"
                     .format(youtube_id)))
                continue

            # We already know the formats are missing from gcs.
            formats_to_create = missing_formats
            if (youtube_id in legacy_mp4_videos and
                    "mp4" in formats_to_create):
                if dryrun:
                    logger.info(
                        "Skipping copy of legacy content due to dryrun")
                else:
                    gcs.copy_legacy_content_to_new_location(youtube_id)
                formats_to_create.remove("mp4")

            if len(formats_to_create) == 0:
                continue

            logger.info("Starting conversion of %s into formats %s" %
                        (youtube_id, ",".join(formats_to_create)))

            if dryrun:
                logger.info(
                    "Skipping downloading and sending job to zencoder due to "
                    "dryrun")
                videos_converted += 1
            else:
                gcs_source_url = gcs.get_or_create_unconverted_source_url(
                    youtube_id)
                if not gcs_source_url:
                    logger.warning("No gcs source URL created for %s; skipping"
                                   % youtube_id)
                    error_ids.append(youtube_id)
                    continue

                try:
                    zencode.start_converting(youtube_id, gcs_source_url,
                                             formats_to_create)
                    videos_converted += 1
                    logger.info(
                        "Video conversion complete with youtube_id %s"
                        % youtube_id
                    )
                except Exception, why:
                    logger.error('Skipping youtube_id "%s": %s'
                                 % (youtube_id, why))
                    error_ids.append(youtube_id)

        backlog_len = len(formats_to_convert) - len(error_ids) \
            - videos_converted
        logger.info(
            'Completed youtube-export with %d in the backlog' % backlog_len
        )

        return (videos_converted, error_ids)


def main():
    parser = optparse.OptionParser()

    parser.add_option("-n", "--no-log",
        action="store_true", dest="nolog",
        help="Log to stdout instead of to a log file", default=False)

    parser.add_option("-m", "--max",
        action="store", dest="max", type="int",
        help="Maximum number of videos to process", default=1)

    parser.add_option("-d", "--dryrun",
        action="store_true", dest="dryrun",
        help="Don't start new zencoder jobs or upload to gcs",
        default=False)

    options, args = parser.parse_args()

    util.setup_logging(options.nolog)

    # Make sure only one youtube-export converter is running at a time.
    with filelock.FileLock("export.lock", timeout=2):
        (success, error_ids) = YouTubeExporter.convert_missing_downloads(
            options.max, options.dryrun)

    if error_ids:
        msg = ('Skipped %d youtube-ids due to errors:\n%s\n'
               % (len(error_ids), '\n'.join(sorted(error_ids))))
        logger.warning(msg)
        # Make this part of the stdout output as well, so it gets passed
        # from cron to our email.
        print msg
    return (success, len(error_ids))


if __name__ == "__main__":
    (_, errors) = main()
    sys.exit(errors)
