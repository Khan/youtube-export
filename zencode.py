from secrets import zencoder_api_key
from zencoder import Zencoder

def start_converting(youtube_id, s3_url, thumbnail_time):
    zen = Zencoder(zencoder_api_key)

    output_config = {
        "base_url": "https://s3.amazonaws.com/KA-youtube-converted/",
        "filename": "%s/%s.m3u8" % (youtube_id, youtube_id),
        "video_codec": "h264",
        "quality": 3,
        "speed": 3,
        "format": "ts",
        "type": "segmented",
        "public": 1,
    }

    if thumbnail_time is not None:
        output_config["thumbnails"] = {
            "times": [thumbnail_time], 
            "public": 1,
            "filename": "%s" % (youtube_id,),
        }

    job = zen.job.create(s3_url, outputs=output_config)

    if job.code == 201:
        print "Zencoder job created successfully"
        return output_config["base_url"] + output_config["filename"]
    else:
        print "Zencoder job creation failed with code %s and body: %s" % (job.code, job.body)
        return None
