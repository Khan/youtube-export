#!/bin/sh

cd $HOME/youtube-export

python export.py --max=250

# The above reports some classes of errors.  Let's report *all* the
# youtube ids that failed to convert.
python print_missing_converted_formats.py
