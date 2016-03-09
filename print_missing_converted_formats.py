#!/usr/bin/env python

import s3


def main():
    yt = s3.list_missing_converted_formats()
    yt = sorted(y for y in yt if yt[y])
    if yt:
        print "MISSING CONVERTED FORMATS:"
        print "\n".join(yt)


if __name__ == '__main__':
    main()
