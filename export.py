import os

import api
import youtube
import s3

class YouTubeExporter(object):

    MAX_CONVERT_PER_RUN = 1
    MAX_UPLOAD_PER_RUN = 1

    @staticmethod
    def start():
        YouTubeExporter.handle_new_videos()
        #YouTubeExporter.handle_converted_videos()

    # Start export for all videos that haven't been converted to downloadable format
    @staticmethod
    def handle_new_videos():
        
        print "Searching for unconverted YouTube videos"

        videos = api.list_new_videos()[:YouTubeExporter.MAX_CONVERT_PER_RUN]
        for video in videos:

            print "Converting youtube id: %s" % video["youtube_id"]

            youtube_id, video_filename, video_path = youtube.download(video)

            print "Downloaded video to: %s" % video_path

            s3_url = s3.upload_unconverted(video_filename, video_path)

            print "Uploaded video to: %s" % s3_url

            #zencoder.start_converting(filename)
            os.remove(video_path)

    # Finish export for all videos that have been converted to downloadable format
    # but haven't been uploaded to public host yet
    @staticmethod
    def handle_converted_videos():
        videos_s3 = s3.list_converted_videos()[:YouTubeExporter.MAX_UPLOAD_PER_RUN]
        for video_s3 in videos_s3:
            archive.upload_from_s3(video_s3)

def main():
    YouTubeExporter.start()
    
if __name__ == "__main__":
    main()
