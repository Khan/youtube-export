#!/bin/sh

cd /home/ec2-user/youtube-export

python export.py --step=publish --max=25
