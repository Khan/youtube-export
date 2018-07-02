#!/bin/sh

# Figure out what directory we're *really* in (following symlinks).
# http://stackoverflow.com/questions/59895/can-a-bash-script-tell-what-directory-its-stored-in
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do   # follow symlinks
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"    # resolve relative symlink
done
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

# Go to export.py's directory
cd $DIR/..

python export.py --max=250

# The above reports some classes of errors.  Let's report *all* the
# youtube ids that failed to convert.
python print_missing_converted_formats.py
