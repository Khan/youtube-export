# Youtube Export

A python script that converts our Youtube videos into several
downloadable formats.

Currently run every 24 hours as a cronjob in our [internal services](https://github.com/khan/internal-services) repo, this script does the following:
  1. Finds the Youtube videos that are missing from S3
  2. Converts them into mp4 and m3u8 format, using Zencoder
  3. Upload the converted files into S3

# Running the Script

In the case that our kubernetes cluster is down and we want to run the script
manually, set the following keys as environment variables (can be retrieved
from webapp's secrets.py):
  1. S3_ACCESS_KEY
  2. S3_SECRET_KEY
  3. ZENCODER_API_KEY

 then run `./cron/convert.sh`.

# Deploying

Since this service is run on our internal services kubernetes cluster, make
sure to go to the [internal services](https://github.com/khan/internal-services)
repo and deploy it with `make deploy` in the youtube-export directory.