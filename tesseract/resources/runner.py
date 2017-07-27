#!/usr/bin/env python

import argparse
import cloudpickle

from io import open


def run(pickled_runner):
    f = cloudpickle.load(open(pickled_runner, "rb"))
    res = f["func"](*f["args"], **f["kwargs"])
    cloudpickle.dump(res, open("./result.pickle", "wb"))
    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("pickled_runner")
    args = parser.parse_args()
    run(args.pickled_runner)
