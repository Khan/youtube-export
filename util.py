import subprocess
import logging

def popen_results(args):
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    results = proc.communicate()

    if results[1]:
        logging.error("Error during popen with command line '%s'" % args)
        logging.error(results[1])

    return results[0]
