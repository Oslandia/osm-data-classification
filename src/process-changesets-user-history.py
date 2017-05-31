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
    fname = "data/raw/changesets-full-17-03-22.csv"
    data = dd.read_csv(fname, blocksize=2**32, dtype=d)
