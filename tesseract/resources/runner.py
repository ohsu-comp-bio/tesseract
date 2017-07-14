#!/usr/bin/env python

import argparse
import cloudpickle


def run(pickled_runner):
    f = cloudpickle.loads(open(pickled_runner, "rb").read())
    res = f["func"](**f["kwargs"])
    res_cp = cloudpickle.dumps(res)
    with open("/tmp/tesseract_result.pickle", "wb") as fh:
        fh.write(res_cp)
    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("pickled_runner")
    args = parser.parse_args()
    run(args.pickled_runner)
