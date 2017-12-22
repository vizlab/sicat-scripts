from __future__ import print_function
from __future__ import division
import os
import os.path
from argparse import ArgumentParser
from glob import glob
import numpy as np
import pygrib


def to_numpy(filename):
    grbs = pygrib.open(filename)
    num_t = grbs.messages // 17
    data = np.empty((num_t, 155, 191))
    for i, grb in enumerate(grbs.select(name='Total Precipitation')):
        data[i] = grb.values
    return data


def main():
    parser = ArgumentParser()
    parser.add_argument('--dest', default='dump',
                        help='destination directory')
    parser.add_argument('files', nargs='+',
                        help='files to be processed')
    args = parser.parse_args()

    if not os.path.exists(args.dest):
        os.makedirs(args.dest)

    for pattern in args.files:
        for filename in glob(pattern):
            basename = os.path.basename(filename)
            print(basename)
            data = to_numpy(filename)
            np.save('{}/{}'.format(args.dest, basename), data)


if __name__ == '__main__':
    main()
