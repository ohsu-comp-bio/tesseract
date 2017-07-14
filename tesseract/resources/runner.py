#!/usr/bin/env python

import argparse
import cloudpickle
import pickle


def run(pickled_func):
    f = pickle.loads(open(pickled_func, "r").read())
    res = f.run()
    res_cp = cloudpickle.dumps(res)
    with open("/tmp/tesseract_result.pickle", "w") as fh:
        fh.write(res_cp)
    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("cloudpickle_file")
    args = parser.parse_args()
    run(args.cloudpickle_file, args.file_store)
