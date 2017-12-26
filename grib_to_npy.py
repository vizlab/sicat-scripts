from __future__ import print_function
from __future__ import division
import os
import os.path
from argparse import ArgumentParser
from glob import glob
import itertools
from multiprocessing import Pool
import numpy as np
import pygrib


def outpath(filename, dest):
    basename = os.path.basename(filename)
    return '{}/{}.npy'.format(dest, basename)


def to_numpy(filename):
    grbs = pygrib.open(filename)
    num_t = grbs.messages // 17
    data = np.empty((num_t, 155, 191))
    for i, grb in enumerate(grbs.select(name='Total Precipitation')):
        data[i] = grb.values
    return data


def process(args):
    filename, dest = args
    data = to_numpy(filename)
    np.save(outpath(filename, dest), data)


def main():
    parser = ArgumentParser()
    parser.add_argument('--dest', default='dump',
                        help='destination directory')
    parser.add_argument('--threads', default=1, type=int,
                        help='number of parallel threads')
    parser.add_argument('files', nargs='+',
                        help='files to be processed')
    args = parser.parse_args()

    if not os.path.exists(args.dest):
        os.makedirs(args.dest)

    filenames = [(f, args.dest) for f
                 in itertools.chain(*[glob(p) for p in args.files])
                 if not os.path.exists(outpath(f, args.dest))]

    pool = Pool(args.threads)
    pool.map(process, filenames)


if __name__ == '__main__':
    main()
