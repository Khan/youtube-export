import logging
import optparse
import os
import time

import api
import youtube
import s3
import zencode

class YouTubeExporter(object):

    MAX_CONVERT_PER_RUN = 1

    # Start export for all videos that haven't been converted to downloadable format
    @staticmethod
    def convert_new_videos():
        logging.info("Searching for unconverted videos")

        videos = api.list_new_videos()[:YouTubeExporter.MAX_CONVERT_PER_RUN]
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
    def publish_converted_videos():
        logging.info("Searching for converted videos")

        videos = api.list_new_videos()

        dict_videos = {}
        for video in videos:
            dict_videos[video["youtube_id"]] = video

        for converted_video in s3.list_converted_videos():
            youtube_id = converted_video["youtube_id"]

            video = dict_videos.get(youtube_id)
            if video and not video["download_urls"]:
                logging.info("Found newly converted video with youtube id %s" % youtube_id)
        
                if api.update_download_available(youtube_id):
                    logging.info("Updated KA download_available")
                else:
                    logging.error("Unable to update KA download_available for youtube id %s" % youtube_id)

def main():

    logging.getLogger().setLevel(logging.DEBUG)
    logging.basicConfig(format='%(relativeCreated)dms %(message)s')

    parser = optparse.OptionParser()

    parser.add_option('-s', '--step',
        action="store", dest="step",
        help="Export step ('convert' or 'publish' currently)", default="convert")

    options, args = parser.parse_args()

    if options.step == "convert":
        YouTubeExporter.convert_new_videos()
    elif options.step == "publish":
        YouTubeExporter.publish_converted_videos()
    else:
        print "Unknown export step."

if __name__ == "__main__":
    main()
