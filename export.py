import optparse
import os
import time
import urllib2
import shutil

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

            s3_url = s3.upload_unconverted_to_s3(youtube_id, video_filename, video_path)
            print "Uploaded video to %s" % s3_url

            os.remove(video_path)
            print "Deleted %s" % video_path

            s3_url_converted = zencode.start_converting(youtube_id, s3_url)
            print "Started converting %s to %s" % (s3_url, s3_url_converted)

    # Finish export for all videos that have been converted to downloadable format
    # but haven't been uploaded to public host yet
    @staticmethod
    def publish_converted_videos():
        print "Searching for converted videos"

        s3_videos = s3.list_converted_videos()[:YouTubeExporter.MAX_PUBLISH_PER_RUN]

        for s3_video in s3_videos:
            s3_folder_url = s3_video["url"]
            youtube_id = s3_video["youtube_id"]
            print "Starting publish with %s (youtube id: %s)" % (s3_folder_url, youtube_id)

            video_folder_path = s3.download_from_s3(youtube_id, s3_folder_url)
            print "Downloaded %s to %s" % (s3_folder_url, video_folder_path)

            archive_bucket_url = s3.upload_converted_to_archive(youtube_id, video_folder_path)
            print "Uploaded via archive.org to %s" % archive_bucket_url

            shutil.rmtree(video_folder_path)
            print "Deleted recursively %s" % video_folder_path

            s3.clean_up_video_on_s3(youtube_id)
            print "Deleted videos from s3 (youtube id: %s)" % youtube_id

            time.sleep(10)
            print "Waited 10 seconds"

            if YouTubeExporter.confirm_success(youtube_id):
                print "Confirmed successful upload to archive.org"

                if api.update_download_available(youtube_id):
                    print "Updated KA download_available"

    @staticmethod
    def confirm_success(youtube_id):
        request = urllib2.Request("http://s3.us.archive.org/KA-converted/%s/%s.m3u8" % (youtube_id, youtube_id))
        request.get_method = lambda: "HEAD"
        response = urllib2.urlopen(request)
        return response.code == 200

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
