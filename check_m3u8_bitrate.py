# TODO(colin): fix these lint errors (http://pep8.readthedocs.io/en/release-1.7.x/intro.html#error-codes)
# pep8-disable:E302,E501,E701
from api import list_videos
import urllib2
import re
from urlparse import urljoin
import csv
import sys
import os

re_url = re.compile(r"\.m3u8$")
re_total_dur = re.compile(r"#ZEN[_-]TOTAL[_-]DURATION:(\d+(?:\.\d+)?)$", re.MULTILINE)
re_segment_name = re.compile(r"^.*\.ts$", re.MULTILINE)

class HeadRequest(urllib2.Request):
    def get_method(self):
        return "HEAD"

header_row = ["youtube_id", "duration", "total_bytes", "kbps"]
existing_youtube_ids = set()
existing_rows = []

if os.path.exists(sys.argv[1]):
    f = open(sys.argv[1], "r")
    for row_num, row in enumerate(csv.reader(f)):
        if row_num == 0:
            assert row == header_row, row
        else:
            existing_youtube_ids.add(row[0])
            existing_rows.append(row)
    f.close()

writer = csv.writer(open(sys.argv[1], "w"))
writer.writerow(header_row)
writer.writerows(existing_rows)

for v in list_videos():
    if v["youtube_id"] in existing_youtube_ids: continue
    
    if "download_urls" not in v: continue
    if v["download_urls"] is None: continue
    
    url = v["download_urls"].get("m3u8", None)
    if url is None: continue
    
    # Munge the URL to get the low-kbps stream
    url = re_url.sub("-low.m3u8", url)
    try:
        doc = urllib2.urlopen(url).read()
        duration_match = re_total_dur.search(doc)
        if duration_match is None:
            print >>sys.stderr, "No duration match for {0}".format(url)
            continue
        duration = float(duration_match.group(1))
        
        total_bytes = 0
        for segment_name in re_segment_name.finditer(doc):
            segment_url = urljoin(url, segment_name.group(0))
            total_bytes += int(urllib2.urlopen(HeadRequest(segment_url)).info()["Content-Length"])
        if total_bytes == 0:
            print >>sys.stderr, "No segments for {0}".format(url)
            continue
    except urllib2.URLError:
        print >>sys.stderr, "URLError for {0}".format(url)
        continue
    
    writer.writerow([v["youtube_id"], duration, total_bytes, (total_bytes / 125.0) / duration])
    existing_youtube_ids.add(v["youtube_id"])
