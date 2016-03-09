import subprocess
import logging


def popen_results(args):
    proc = subprocess.Popen(args,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    results = proc.communicate()

    if results[1]:
        logger.error("Error during popen with command line '%s'" % args)
        logger.error(results[1])

    return results[0]

logger = logging.getLogger("khan")
logger.addHandler(logging.StreamHandler())


DOWNLOADABLE_FORMATS = set(["mp4", "m3u8"])
