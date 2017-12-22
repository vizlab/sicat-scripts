from __future__ import print_function
from __future__ import division
import os
import os.path
import re
from argparse import ArgumentParser
from datetime import datetime, timedelta
import json
import csv
from glob import glob
import numpy as np


def month_hours(year, month):
    d = datetime(year, month, 1, 1, 0)
    while d.month == month:
        yield d
        d += timedelta(hours=1)
    yield d


def process_file(filename, shapes, dest):
    basename = os.path.basename(filename)[:-4]
    print(basename)

    pattern = r'surf_(.+)_(m[0-9]{3})_([0-9]{4})([0-9]{2}).grib'
    model, ensemble, year, month = re.match(pattern, basename).groups()
    year = int(year)
    month = int(month)

    data = np.load(filename)
    ts = list(month_hours(year, month))

    writer = csv.writer(open('{}/{}.csv'.format(dest, basename), 'w'))
    for shape in shapes:
        indices = shape['point_indices']
        if not indices['lat']:
            continue
        values = data[:, indices['lat'], indices['lon']]
        count = len(values[0])
        result_min = np.min(values, axis=1)
        result_max = np.max(values, axis=1)
        result_sum = np.sum(values, axis=1)
        result_sumsq = np.sum(values[:] ** 2, axis=1)
        writer.writerows([[
            basename,
            shape['code'],
            t,
            count,
            result_min[i],
            result_max[i],
            result_sum[i],
            result_sumsq[i],
        ] for i, t in enumerate(ts)])


def main():
    parser = ArgumentParser()
    parser.add_argument('--dest', default='result',
                        help='destination directory')
    parser.add_argument('--target', nargs=1,
                        help='target geometry file')
    parser.add_argument('files', nargs='+',
                        help='.npy files')
    args = parser.parse_args()

    if not os.path.exists(args.dest):
        os.makedirs(args.dest)

    shapes = json.load(open(args.target[0]))

    for pattern in args.files:
        for filename in glob(pattern):
            process_file(filename, shapes, args.dest)


if __name__ == '__main__':
    main()
