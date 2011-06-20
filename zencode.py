import logging

from secrets import zencoder_api_key
from zencoder import Zencoder

BASE_URL = "https://s3.amazonaws.com/KA-youtube-converted/"

def start_converting(youtube_id, s3_url, thumbnail_time):
    zen = Zencoder(zencoder_api_key)

    output_config = {
        "base_url": BASE_URL,
        "filename": "%s/%s.mp4" % (youtube_id, youtube_id),
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
        output_config["thumbnails"] = {
            "base_url": BASE_URL + youtube_id,
            "times": [thumbnail_time], 
            "public": 1,
            "filename": "%s" % youtube_id,
        }

    job = zen.job.create(s3_url, outputs=output_config)

    assert(job.code == 201)

    logging.info("Zencoder job created successfully")
    return output_config["base_url"] + output_config["filename"]
