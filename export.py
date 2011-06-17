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
        print "Searching for unconverted videos"

        videos = api.list_new_videos()[:YouTubeExporter.MAX_CONVERT_PER_RUN]
        for video in videos:
            print "Starting conversion with youtube id %s" % video["youtube_id"]

            youtube_id, video_filename, video_path = youtube.download(video)
            print "Downloaded video to %s" % video_path

            s3_url = s3.upload_unconverted_to_s3(youtube_id, video_filename, video_path)
            print "Uploaded video to %s" % s3_url

            os.remove(video_path)
            print "Deleted %s" % video_path

            s3_url_converted = zencode.start_converting(youtube_id, s3_url)
            print "Started converting %s to %s" % (s3_url, s3_url_converted)

    # Finish export for all videos that have been converted to downloadable format
    @staticmethod
    def finish_converted_videos():
        print "Searching for converted videos"

        videos = api.list_new_videos()

        dict_videos = {}
        for video in videos:
            dict_videos[video["youtube_id"]] = video

        for converted_video in s3.list_converted_videos():
            youtube_id = converted_video["youtube_id"]

            video = dict_videos.get(youtube_id)
            if video and not video["download_url"]:
                print "Found newly converted video with youtube id %s" % youtube_id
        
                if api.update_download_available(youtube_id):
                    print "Updated KA download_available"

def main():

    parser = optparse.OptionParser()

    parser.add_option('-s', '--step',
        action="store", dest="step",
        help="Export step ('convert' or 'finish' currently)", default="convert")

    options, args = parser.parse_args()

    if options.step == "convert":
        YouTubeExporter.convert_new_videos()
    elif options.step == "finish":
        YouTubeExporter.finish_converted_videos()
    else:
        print "Unknown export step."

if __name__ == "__main__":
    main()
