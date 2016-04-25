#!/usr/bin/env python

import os
import json
import time

import s3
import util


def main():
    util.setup_logging()

    util.logger.info('Running s3.list_missing_converted_formats()')
    today_yt = s3.list_missing_converted_formats()

    # Because we will always be missing formats for new youtube videos
    # that are still in the process of being converted, we only
    # complain for videos that have ben missing a converted format for
    # at least 2 days.
    yesterday_fname = '/tmp/last_missing_converted.json'
    if not os.path.exists(yesterday_fname):
        util.logger.warn("Missing %s, will not report any missing converted "
                         "formats today." % yesterday_fname)
        yesterday_yt = {}
    else:
        with open(yesterday_fname) as f:
            yesterday_yt = json.load(f)

    # This limits the output to keys/values in both yesterday *and* today.
    yt = {k: set(yesterday_yt[k]) & set(today_yt[k])
          for k in set(yesterday_yt) & set(today_yt)}

    yt = sorted("%s: %s" % (y, sorted(yt[y])) for y in yt if yt[y])
    if yt:
        util.logger.error("MISSING CONVERTED FORMATS:\n%s" % "\n".join(yt))

    # Now write today's output out for tomorrow.  We only do this update
    # once a day, where we round 'day' to 20 hours.  We need to convert
    # our set to a list before we can emit it.
    json_yt = {k: sorted(v) for (k, v) in today_yt.iteritems()}
    if (not os.path.exists(yesterday_fname) or
            os.path.getmtime(yesterday_fname) + 20 * 60 * 60 < time.time()):
        util.logger.info('Saving converted-formats output for use tomorrow')
        with open(yesterday_fname, 'w') as f:
            json.dump(json_yt, f, indent=4, sort_keys=True)


if __name__ == '__main__':
    main()
