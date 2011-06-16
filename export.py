import optparse
import os

import api
import youtube
import s3
import zencode

class YouTubeExporter(object):

    MAX_CONVERT_PER_RUN = 1
    MAX_PUBLISH_PER_RUN = 1

    # Start export for all videos that haven't been converted to downloadable format
    @staticmethod
    def convert_new_videos():
        print "Searching for unconverted videos"

        videos = api.list_new_videos()[:YouTubeExporter.MAX_CONVERT_PER_RUN]
        for video in videos:
            print "Starting conversion with youtube id %s" % video["youtube_id"]

            youtube_id, video_filename, video_path = youtube.download(video)
            print "Downloaded video to %s" % video_path

            s3_url = s3.upload_unconverted(youtube_id, video_filename, video_path)
            print "Uploaded video to %s" % s3_url

            os.remove(video_path)
            print "Deleted %s" % video_path

            s3_url_converted = zencode.start_converting(s3_url, youtube_id)
            print "Started converting %s to %s" % (s3_url, s3_url_converted)

    # Finish export for all videos that have been converted to downloadable format
    # but haven't been uploaded to public host yet
    @staticmethod
    def publish_converted_videos():
        print "Searching for converted videos"

        s3_urls = s3.list_converted_video_urls()[:YouTubeExporter.MAX_PUBLISH_PER_RUN]
        for s3_url in s3_urls:
            print "Starting publish with %s" % s3_url

            archive_url = archive.upload_from_s3(s3_url)
            print "Uploaded via archive.org to %s" % archive_url

            # TODO: check HEAD request on archive.org
            # TODO: delete both versions from s3
            # TODO: update KA API record

def main():

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
