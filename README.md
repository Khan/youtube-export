# Youtube Export

A python script that converts our Youtube videos into several
downloadable formats.

Currently run every 24 hours as a cronjob in our [internal services](https://github.com/khan/internal-services) repo,
This script does the following:
  1. Finds the Youtube videos that are missing from GCS
  2. Converts them into mp4 and m3u8 format, using Zencoder
  3. Upload the converted files into GCS

# Running the Script

In the case that our kubernetes cluster is down and we want to run the script
manually, set the following keys as environment variables:
  1. GOOGLE_APPLICATION_CREDENTIALS
  2. ZENCODER_API_KEY

 then run `./cron/convert.sh`.

# Deploying

Since this service is run on our internal services kubernetes cluster, make
sure to go to the [internal services](https://github.com/khan/internal-services)
repo and deploy it with `make deploy` in the youtube-export directory.

# Monitoring

Monitoring for this job can be found [here](https://app.google.stackdriver.com/dashboards/17890615029290716410?project=khan-academy&timeDomain=1w).