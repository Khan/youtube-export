import logging
import os
import youtube

from zencoder import Zencoder
from util import logger

zencoder_api_key = open(os.path.expanduser('~/zencoder_api_key')).read().strip()

BASE_URL = "https://s3.amazonaws.com/KA-youtube-converted/"

def output_types():
    return {
        "mp4": [output_mp4],

        # NOTE: Experimental aggressive compression settings -- not to be part
        #     of production processing yet.
        "mp4_aggressive_compression_test": [output_mp4_low],

        "m3u8": [
            output_m3u8_playlist,
            output_m3u8_low,
            output_m3u8_medium,
            output_m3u8_high,
        ],

        # NOTE: Experimental aggressive compression settings -- not to be part
        #     of production processing yet.
        "m3u8_aggressive_compression_test": [
            output_m3u8_playlist_aggressively_compressed,
            output_m3u8_aggressively_compressed,
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
        "filename": "%s.mp4/%s-low.mp4" % (youtube_id, youtube_id),
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

    # TODO(david): The output folder+filename is the same in this output
    #    function as output_mp4 below. This could mean that if we process with
    #    output_mp4 then output_mp4_low, we'll end up with the low-quality
    #    thumbnail for the regular mp4 video too. However, thumbnail extraction
    #    is not currently working. If we do want to make it work, look into
    #    this.
    if thumbnail_time is not None:
        output["thumbnails"] = {
            "times": [thumbnail_time],
            "public": 1,
            "base_url": "{0}{1}.mp4/".format(base_url, youtube_id),
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
        "skip_video": True,

        # Audio encoding options
        "audio_channels": 1,
        "audio_quality": 3,
        "audio_normalize": True,
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

def output_m3u8_playlist_aggressively_compressed(
        youtube_id, thumbnail_time, base_url):
    return {
        "public": 1,
        "base_url": base_url,
        "filename": "%s.m3u8/%s-compressed.m3u8" % (youtube_id, youtube_id),
        "streams": [{
            "bandwidth": 64,
            "path": "%s-compressed.m3u8" % youtube_id
        }],
        "type": "playlist"
    }

def output_m3u8_aggressively_compressed(youtube_id, thumbnail_time, base_url):
    return {
        "base_url": base_url,
        "filename": "%s.m3u8/%s-compressed.m3u8" % (youtube_id, youtube_id),
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
