#!/usr/bin/env python2

"""Print the JSON output of a given 'output_*' function in zencode.py.

The 'output_*' functions in zencode.py generate JSON configurations which
control Zencoder.com's behavior.  This script provides an easy mechanism to
preview those JSON configurations (without actually submitting a Zencoder job).

Example usage:
    ./test_print_zencode_config.py output_mp4_low
"""

import argparse
import json

import zencode


def pretty_print_dict(json_dict):
    """Print a JSON dictionary using "pretty print" formatting."""
    print json.dumps(json_dict, sort_keys=True, indent=4,
                     separators=(',', ': '))


def parse_arguments():
    """Parse the command-line arguments.

    One positional argument (the zencode.py 'output_*' function name) is
    required.
    """
    parser = argparse.ArgumentParser(
        description="Print JSON configurations sent to Zencoder.com")

    parser.add_argument('function_name', help="The name of the zencode.py "
                        "config function to test (e.g. 'output_mp4_low'")

    return parser.parse_args()


def print_zencode_config_with_fake_args(function):
    """Evaluate and print a zencode.py 'output_*' config function.

    Arguments:
        function: a pointer to the 'output_*' function to call.
    """
    youtube_id = "FAKE_YTB_ID"
    thumbnail_time = 42
    base_url = "gcs://FAKE_S3_BUCKET/"
    zencoder_config = function(youtube_id, thumbnail_time, base_url)
    pretty_print_dict(zencoder_config)


def main():
    args = parse_arguments()
    function = getattr(zencode, args.function_name)
    print_zencode_config_with_fake_args(function)


if __name__ == "__main__":
    main()
