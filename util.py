import datetime
import logging
import os
import subprocess
import sys

DOWNLOADABLE_FORMATS = set(["mp4", "m3u8"])


def popen_results(args):
    proc = subprocess.Popen(args,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    results = proc.communicate()

    if results[1]:
        logger.error("Error during popen with command line '%s'" % args)
        logger.error(results[1])

    return results[0]


logger = logging.getLogger("khan")


def setup_logging(only_log_to_stdout=False, log_level=logging.DEBUG):
    assert not logger.handlers, "Should only call setup_logging once."

    logger.setLevel(log_level)
    formatter = logging.Formatter(fmt='%(relativeCreated)dms %(message)s')

    def _add_handler(handler, level):
        handler.setFormatter(formatter)
        handler.setLevel(level)
        logger.addHandler(handler)

    if only_log_to_stdout:
        _add_handler(logging.StreamHandler(), log_level)
    else:
        if not os.path.isdir("logs"):
            os.mkdir("logs")

        strftime = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        _add_handler(logging.FileHandler('logs/convert_%s.log' % strftime),
                     log_level)

        # When logging to logs, also log warnings and serious errors to stderr
        # (that way they're picked up by cron mail).
        _add_handler(logging.StreamHandler(), logging.WARNING)

        # Print to stdout on INFO level logs so that Stackdriver will log them.
        # TODO(yeva): Use the google-cloud-logging package for logging.
        info_logging_handler = logging.StreamHandler(stream=sys.stdout)
        info_logging_handler.addFilter(_SingleLevelFilter(logging.INFO))
        _add_handler(info_logging_handler, level=logging.INFO)


class _SingleLevelFilter(logging.Filter):
    def __init__(self, level):
        self.level = level

    def filter(self, record):
        return record.levelno == self.level
