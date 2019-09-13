# coding: utf-8

"""Count the number changesets by user, i.e. uid

This script used the dask library to handle the large input CSV file, ~210Go,
about the changesets history.
"""


import numpy as np

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
    parser.add_argument('analyze', help='type of the analyze: changeset or editor', type=str)
    parser.add_argument('-o', '--output', help='name of the output file')
    args = parser.parse_args()

    analyze = args.analyze
    if analyze not in ['changeset', 'editor']:
        raise ValueError("wrong analyze name: 'changeset' or 'editor'")

    if args.output is None:
        outpath = os.path.join('./data', 'output-extracts', 'all-' + analyze  + 's-by-user.csv')
    print(outpath)

    fname = "data/output-extracts/changesets-full-17-03-22.csv"
    print("dask read the CSV '{}'".format(fname))
    data = dd.read_csv(fname, blocksize=2**32, dtype=DTYPE)

    print("data processing")
    if analyze == 'changeset':
        result = nb_changeset_by_uid(data)
    if analyze == 'editor':
        result = distinct_software_by_uid(data)
    print("writing results")
    result.to_csv(outpath)
