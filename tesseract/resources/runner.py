#!/usr/bin/env python

import argparse
import cloudpickle


def run(pickled_runner):
    f = cloudpickle.load(open(pickled_runner, "rb"))
    res = f["func"](**f["kwargs"])
    cloudpickle.dump(res, open("/tmp/tesseract_result.pickle", "wb"))
    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("pickled_runner")
    args = parser.parse_args()
    run(args.pickled_runner)
