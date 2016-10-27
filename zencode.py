import os

from zencoder import Zencoder
from util import logger

zencoder_api_key = (open(os.path.expanduser('~/zencoder_api_key')).read()
                    .strip())

BASE_URL = "https://s3.amazonaws.com/KA-youtube-converted/"


def output_types():
    return {
        "mp4": [
            output_mp4_low,
            output_mp4,
        ],
        "mp4_low_only": [
            output_mp4_low,
        ],
        "mp4_low_ios_only": [
            output_mp4_low_ios,
        ],
        "m3u8": [
            output_m3u8_playlist,
            output_m3u8_low,
            output_m3u8_medium,
            output_m3u8_high,
        ],
        "m3u8_low_only": [
            output_m3u8_low,
        ],
    }


def start_converting(youtube_id, s3_url, formats_to_create, base_url=BASE_URL):

    # TODO(csilvers): figure out how to get thumbnail times from youtube APIv3
    #thumbnail_time = youtube.get_thumbnail_time(youtube_id)
    thumbnail_time = None

    zen = Zencoder(zencoder_api_key)
    outputs = []

    for format_to_create in formats_to_create:
        if format_to_create in output_types():
            outputs += [fxn(youtube_id, thumbnail_time, base_url)
                    for fxn in output_types()[format_to_create]]
    
    job_response = zen.job.create(s3_url, outputs=outputs)

    assert job_response.code == 201, job_response.body

    logger.info("Zencoder job created successfully")


def output_mp4_low(youtube_id, thumbnail_time, base_url):
    output = {
        "base_url": base_url,

        # Why is the filename format %s.mp4-low/%s-low.mp4 instead of a more
        # consistent %s-low.mp4/%s-low.mp4?
        #
        # [webapp repo]/content/publish.py:update_converted_videos_from_S3()
        # expects folders of the format YOUTUBE_ID.FORMAT -- it infers the
        # YOUTUBE_ID from the part before the dot. So if there is a YOUTUBE_ID
        # that ends in -low, there is the (low) possibility of collision here.
        # Also, this simplifies changes to update_converted_videos_from_S3() to
        # extract the presence of this low-size mp4 download URL.
        "filename": "%s.mp4-low/%s-low.mp4" % (youtube_id, youtube_id),

        "public": 1,
        "speed": 1,
        "tuning": "animation",

        # We're using the 3GP container format which supports audio streams
        # encoded with the AMR codec.
        "format": "3gp",

        # Video encoding options
        "video_codec": "h264",
        "crf": 40,
        "max_frame_rate": 15,
        "size": "640x480",

        # Audio encoding options
        "audio_codec": "amr",
        "audio_bitrate": 4,
        "audio_sample_rate": 8000,

        "watermarks": [
            {
                "width": 128,
                "height": 16,
                "x": -2,
                "y": -2,
                "url": "http://www.khanacademy.org/images/watermark.png",
            }
        ]
    }

    if thumbnail_time is not None:
        output["thumbnails"] = {
            "times": [thumbnail_time],
            "public": 1,
            "base_url": "{0}{1}.mp4-low/".format(base_url, youtube_id),
            "filename": youtube_id,
        }

    return output


def output_mp4_low_ios(youtube_id, thumbnail_time, base_url):
    """Zencoder configuration for low-bitrate iOS downloadable videos.

    Previously, we started using the AMR audio codec for our "mp4-low" videos.
    Unfortunately, iOS devices (since iOS 4.3) no longer support AMR playback.
    This configuration instead uses a low-bitrate AAC configuration.

    See https://docs.google.com/document/d/
            1yKz92Vx4nxWt-9h-xRut7CipNP1FP3E1YpX3pL0QubM
    """

    # TODO(pepas): DRY out all of these output_* functions.

    # Why is the filename format %s.mp4-low/%s-low.mp4 instead of a more
    # consistent %s-low.mp4/%s-low.mp4?
    #
    # [webapp repo]/content/publish.py:update_converted_videos_from_S3()
    # expects folders of the format YOUTUBE_ID.FORMAT -- it infers the
    # YOUTUBE_ID from the part before the dot. So if there is a YOUTUBE_ID
    # that ends in -low, there is the (low) possibility of collision here.
    # Also, this simplifies changes to update_converted_videos_from_S3() to
    # extract the presence of this low-size mp4 download URL.
    destination_directory = "%s.mp4-low-ios" % youtube_id
    destination_filename = "%s-low-ios.mp4" % youtube_id

    # See https://app.zencoder.com/docs/api/encoding
    output = {
        "strict": True,

        "base_url": base_url,
        "filename": "%s/%s" % (destination_directory, destination_filename),

        "public": 1,
        "speed": 1,
        "tuning": "animation",

        "format": "mp4",

        # Video encoding options
        "video_codec": "h264",
        "crf": 40,
        "max_frame_rate": 15,
        "size": "640x480",

        # Audio encoding options
        # Note(pepas): These settings are based on `output_m3u8_low`, but with
        # the bitrate bumped up to 16kbit.  See the encoding comparison at
        # https://console.aws.amazon.com/s3/home?region=us-east-1#
        #     &bucket=ka-jason-test-bucket&prefix=FXSuEIMrPQk.mp4-low/
        "audio_codec": "aac",
        "audio_bitrate": 16,
        "audio_channels": 1,
        "audio_normalize": True,
        "audio_lowpass": 6000,

        "watermarks": [
            {
                "width": 128,
                "height": 16,
                "x": -2,
                "y": -2,
                "url": "http://www.khanacademy.org/images/watermark.png",
            }
        ]
    }

    if thumbnail_time is not None:
        output["thumbnails"] = {
            "times": [thumbnail_time],
            "public": 1,
            "base_url": "{0}{1}/".format(base_url, destination_directory),
            "filename": youtube_id,
        }

    return output


def output_mp4(youtube_id, thumbnail_time, base_url):
    output = {
        "base_url": base_url,
        "filename": "%s.mp4/%s.mp4" % (youtube_id, youtube_id),
        "public": 1,
        "speed": 1,
        "tuning": "animation",

        # Video encoding options
        "video_codec": "h264",
        "quality": 5,

        "watermarks": [
            {
                "width": 128,
                "height": 16,
                "x": -2,
                "y": -2,
                "url": "http://www.khanacademy.org/images/watermark.png",
            }
        ]
    }

    if thumbnail_time is not None:
        output["thumbnails"] = {
            "times": [thumbnail_time], 
            "public": 1,
            "base_url": "{0}{1}.mp4/".format(base_url, youtube_id),
            "filename": youtube_id,
        }

    return output


def output_m3u8_playlist(youtube_id, thumbnail_time, base_url):
    return {
        "public": 1,
        "base_url": base_url,
        "filename": "%s.m3u8/%s.m3u8" % (youtube_id, youtube_id),
        "streams": [
            {
                "bandwidth": 640,
                "path": "%s-high.m3u8" % youtube_id
            },
            {
                "bandwidth": 160,
                "path": "%s-medium.m3u8" % youtube_id
            },
            {
                "bandwidth": 64,
                "path": "%s-low.m3u8" % youtube_id
            },
        ],
        "type": "playlist"
    }


def output_m3u8_low(youtube_id, thumbnail_time, base_url):
    return {
        "base_url": base_url,
        "filename": "%s.m3u8/%s-low.m3u8" % (youtube_id, youtube_id),
        "public": 1,
        "type": "segmented",

        # Video encoding options
        "video_codec": "h264",
        "crf": 40,
        "max_frame_rate": 15,
        "size": "640x480",

        # Audio encoding options
        "audio_codec": "aac",
        "audio_bitrate": 8,  # Zencoder can't do lower than 8
        "audio_channels": 1,
        "audio_normalize": True,
        "audio_lowpass": 6000,

        "watermarks": [
            {
                "width": 128,
                "height": 16,
                "x": -2,
                "y": -2,
                "url": "http://www.khanacademy.org/images/watermark.png",
            }
        ]
    }


def output_m3u8_medium(youtube_id, thumbnail_time, base_url):
    return {
        "public": 1,
        "base_url": base_url,
        "bitrate_cap": 120,
        "buffer_size": 480,
        "filename": "%s.m3u8/%s-medium.m3u8" % (youtube_id, youtube_id),
        "type": "segmented",

        # Video encoding options
        "video_codec": "h264",
        "quality": 4,
        "max_video_bitrate": 100,

        # Audio encoding options
        "audio_quality": 3,
        "audio_normalize": True,

        "watermarks": [
            {
                "width": 128,
                "height": 16,
                "x": -2,
                "y": -2,
                "url": "http://www.khanacademy.org/images/watermark.png",
            }
        ]
    }


def output_m3u8_high(youtube_id, thumbnail_time, base_url):
    return {
        "public": 1,
        "base_url": base_url,
        "filename": "%s.m3u8/%s-high.m3u8" % (youtube_id, youtube_id),
        "type": "segmented",

        # Video encoding options
        "video_codec": "h264",
        "quality": 4,

        # Audio encoding options
        "audio_quality": 3,
        "audio_normalize": True,

        "watermarks": [
            {
                "width": 128,
                "height": 16,
                "x": -2,
                "y": -2,
                "url": "http://www.khanacademy.org/images/watermark.png",
            }
        ]
    }
