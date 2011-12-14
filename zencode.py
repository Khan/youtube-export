import logging
import youtube

from secrets import zencoder_api_key
from zencoder import Zencoder
from util import logger

BASE_URL = "https://s3.amazonaws.com/KA-youtube-converted/"

def output_types():
    return {
        "mp4": [output_mp4],
        "m3u8": [
            output_m3u8_playlist,
            output_m3u8_low,
            output_m3u8_medium,
            output_m3u8_high,
        ]
    }

def start_converting(youtube_id, s3_url, formats_to_create):

    thumbnail_time = youtube.get_thumbnail_time(youtube_id)
    assert thumbnail_time

    zen = Zencoder(zencoder_api_key)
    outputs = []

    for format_to_create in formats_to_create:
        if format_to_create in output_types():
            outputs += [fxn(youtube_id, thumbnail_time) for fxn in output_types()[format_to_create]]
    
    job = zen.job.create(s3_url, outputs=outputs)

    assert(job.code == 201)

    logger.info("Zencoder job created successfully")

def output_mp4(youtube_id, thumbnail_time):
    output = {
        "base_url": BASE_URL,
        "filename": "%s.mp4/%s.mp4" % (youtube_id, youtube_id),
        "video_codec": "h264",
        "tuning": "animation",
        "quality": 5,
        "speed": 1,
        "public": 1,
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
            "base_url": BASE_URL + youtube_id,
            "times": [thumbnail_time], 
            "public": 1,
            "filename": "%s" % youtube_id,
        }

    return output

def output_m3u8_playlist(youtube_id, thumbnail_time):
    return {
		"public": 1,
		"base_url": BASE_URL,
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

def output_m3u8_low(youtube_id, thumbnail_time):
    return {
		"video_codec": "h264",
		"public": 1,
		"audio_channels": 1,
		"audio_sample_rate": 44100,
        "audio_bitrate": 24,
        "audio_lowpass": 16000,
        "audio_normalize": True,
		"width": 440,
        "quality": 3,
        "bitrate_cap": 36,
        "buffer_size": 144,
		"video_bitrate": 30,
		"type": "segmented",
        "base_url": BASE_URL,
        "filename": "%s.m3u8/%s-low.m3u8" % (youtube_id, youtube_id),
        "watermarks": [
            {
                "width": 64,
                "height": 8,
                "x": -2,
                "y": -2,
                "url": "http://www.khanacademy.org/images/watermark.png",
            }
        ]
    }

def output_m3u8_medium(youtube_id, thumbnail_time):
    return {
		"video_codec": "h264",
		"public": 1,
        "audio_bitrate": 48,
        "audio_normalize": True,
        "quality": 4,
        "bitrate_cap": 120,
        "buffer_size": 480,
		"max_video_bitrate": 100,
		"type": "segmented",
        "base_url": BASE_URL,
        "filename": "%s.m3u8/%s-medium.m3u8" % (youtube_id, youtube_id),
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

def output_m3u8_high(youtube_id, thumbnail_time):
    return {
		"video_codec": "h264",
		"public": 1,
        "audio_bitrate": 48,
        "audio_normalize": True,
        "quality": 4,
		"type": "segmented",
        "base_url": BASE_URL,
        "filename": "%s.m3u8/%s-high.m3u8" % (youtube_id, youtube_id),
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
