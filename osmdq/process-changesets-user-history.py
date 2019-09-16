# coding: utf-8

"""Count the number changesets by user, i.e. uid

This script used the dask library to handle the large input CSV file, +30Gb,
about the changesets history.
"""


import numpy as np

import dask.config
from dask import dataframe as dd


# Column types of the input CSV file
DTYPE = {'id': np.dtype(int),
         'created': np.dtype(str),
         'uid': np.dtype(int),
         'min_lat': np.dtype(float),
         'min_lon': np.dtype(float),
         'max_lat': np.dtype(float),
         'max_lon': np.dtype(float),
         'num_changes': np.dtype(int),
         'comments_count': np.dtype(int),
         'key': np.dtype(str),
         'value': np.dtype(str)}


def nb_changeset_by_uid(data):
    """count the number of changesets by user id.

    data: dask.dataframe

    return a pandas.DataFrame
    """
    grp = (data.drop_duplicates(subset=['id'])
           .groupby('uid')['uid']
           .count())
    return grp.compute()


def distinct_software_by_uid(data):
    """retrieve the software used to edited OSM by user

    return a multi-index pandas.DataFrame
    """
    grp = (data[data.key == 'created_by']
           .groupby(['uid', 'value'])['value']
           .count())
    return grp.compute()


if __name__ == '__main__':
    import os
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('analyze', help='type of the analyze: changeset or editor', type=str, choices=("changeset", "editor"))
    parser.add_argument('-i', '--input', required=True, help='name of the input csv file (changesets history)')
    parser.add_argument('-o', '--output', help='name of the output file')
    parser.add_argument('--blocksize', type=int, default=500, help='Blocksize of chunk (Dask) in MB')
    parser.add_argument('--num-workers', type=int, default=8, help='Number of workers (Dask)')
    args = parser.parse_args()

    dask.config.set(num_workers=args.num_workers)

    analyze = args.analyze

    if args.output is None:
        outpath = os.path.join('./data', 'output-extracts', 'all-' + analyze  + 's-by-user.csv')

    if not os.path.isfile(args.input):
        print("The file '{}' does not exist.".format(args.input))
        parser.exit(1)

    print("dask read the CSV '{}'".format(args.input))
    blocksize = args.blocksize * 1e6  # chunks in MB
    data = dd.read_csv(args.input, blocksize=blocksize, dtype=DTYPE)

    print("data processing")
    if analyze == 'changeset':
        result = nb_changeset_by_uid(data)
    if analyze == 'editor':
        result = distinct_software_by_uid(data)
    print("writing results")
    result.to_csv(args.output)
    print("output file '{}'".format(args.output))
