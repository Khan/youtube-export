import secrets
from zencoder import Zencoder

def start_converting(s3_url, youtube_id):
    zen = Zencoder(secrets.zencoder_api_key)

    output_config = {
      "base_url": "https://s3.amazonaws.com/KA-youtube-converted/",
      "filename": "%s/%s.mp4" % (youtube_id, youtube_id),
      "video_codec": "mpeg4",
      "quality": 3,
      "speed": 3,
      "type": "segmented",
    }

    job = zen.job.create(s3_url, outputs=output_config)

    if job.code == 201:
        print "Zencoder job created successfully"
        return output_config["base_url"] + output_config["filename"]
    else:
        print "Zencoder job creation failed with code %s and body: %s" % (job.code, job.body)
        return None
