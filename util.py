import datetime
import logging
import os
import subprocess


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

        # When logging to logs, also log serious errors to stderr
        # (that way they're picked up by cron mail).
        _add_handler(logging.StreamHandler(), logging.ERROR)
