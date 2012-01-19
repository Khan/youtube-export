#!/bin/sh

cd /home/ec2-user/youtube-export

python export.py --no-archive --step=publish --max=50
