#!/usr/bin/env python

import s3
import util


def main():
    util.setup_logging()

    util.logging.info('Running s3.list_missing_converted_formats()')
    yt = s3.list_missing_converted_formats()

    yt = sorted(y for y in yt if yt[y])
    if yt:
        util.logging.error("MISSING CONVERTED FORMATS:\n%s" % "\n".join(yt))


if __name__ == '__main__':
    main()
