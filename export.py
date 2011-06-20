import datetime
import logging
import optparse
import os
import time

import api
import youtube
import s3
import zencode

class YouTubeExporter(object):

    # Start export for all videos that haven't been converted to downloadable format
    @staticmethod
    def convert_new_videos(max_videos):
        logging.info("Searching for unconverted videos")

        videos = api.list_new_videos()[:max_videos]
        for video in videos:
            logging.info("Starting conversion with youtube id %s" % video["youtube_id"])

            youtube_id, video_path, thumbnail_time = youtube.download(video)
            logging.info("Downloaded video to %s" % video_path)

            assert(youtube_id)
            assert(video_path)

            s3_url = s3.upload_unconverted_to_s3(youtube_id, video_path)
            logging.info("Uploaded video to %s" % s3_url)

            os.remove(video_path)
            logging.info("Deleted %s" % video_path)

            assert(s3_url)

            s3_url_converted = zencode.start_converting(youtube_id, s3_url, thumbnail_time)
            logging.info("Started converting %s to %s" % (s3_url, s3_url_converted))

            assert(s3_url_converted)

    # publish export for all videos that have been converted to downloadable format
    @staticmethod
    def publish_converted_videos(max_videos):
        logging.info("Searching for converted videos")

        videos = api.list_new_videos()

        dict_videos = {}
        for video in videos:
            dict_videos[video["youtube_id"]] = video

        c_publish_attempts = 0

        for converted_video in s3.list_converted_videos():
            youtube_id = converted_video["youtube_id"]

            video = dict_videos.get(youtube_id)
            if video and not video["download_urls"]:

                if c_publish_attempts >= max_videos:
                    break

                logging.info("Found newly converted video with youtube id %s" % youtube_id)
        
                if s3.upload_converted_to_archive(youtube_id):
                    logging.info("Successfully uploaded to archive.org")

                    if api.update_download_available(youtube_id):
                        logging.info("Updated KA download_available")
                    else:
                        logging.error("Unable to update KA download_available for youtube id %s" % youtube_id)

                else:
                    logging.error("Unable to upload to archive.org")

                c_publish_attempts += 1

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

    options, args = parser.parse_args()

    setup_logging(options)

    if options.step == "convert":
        YouTubeExporter.convert_new_videos(options.max)
    elif options.step == "publish":
        YouTubeExporter.publish_converted_videos(options.max)
    else:
        print "Unknown export step."

if __name__ == "__main__":
    main()
