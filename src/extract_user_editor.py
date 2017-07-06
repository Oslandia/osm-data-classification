# coding: utf-8

"""Read and process the occurrence of OSM editors used by the users (from a CSV
file) : uid,value,num

Such as:

- uid: user id
- value: name of the editor as string
- num: number of occurrence
"""

import re
import logging

import numpy as np
import pandas as pd


FORMAT = '%(asctime)s :: %(levelname)s :: %(funcName)s : %(message)s'
logging.basicConfig(format=FORMAT, level=logging.INFO)
logger = logging.getLogger(__name__)


# multiple cases  (where X, Y are digits, 'l' a letter):
#   - name/X.Yl
#   - name X.Yl
#   - name X
#   - name X.Y.Z
#   - name other stuff X.Y.Z-l

DISCARD_CHAR = '!?#~,()[]+'
table = str.maketrans(DISCARD_CHAR, " " * len(DISCARD_CHAR))


def editor_name(value):
    """Try to extract the full name of the editor

    value: str
        name of used OSM editor extracted from the changeset history

    Return a string
    """
    # find the first digit occurrence and take the left-side without space or
    # '/'
    pattern = r"([\sa-zA-Z4_\.]+).*"
    value = value.lower().translate(table)
    return "".join(re.split(pattern, value)).strip()


def main(fname, output_fname):
    logger.info("read the CSV file '%s'", fname)
    df = pd.read_csv(fname, header=None, names=['uid', 'value', 'num'])
    logger.info("retrieve the unique editor name")
    df['fullname'] = df['value'].apply(editor_name)
    logger.info("group by uid,fullname")
    # df.groupby(['uid', 'fullname'])['num'].sum()
    return df


# what to extract
# - number of unique editor used by user (high value => experienced user?)

# two choices
# simple one but count 4 JOSM if JOSM has been used 4 times by a user
# (df.groupby('fullname')['num']
#  .count()
#  .sort_values(ascending=False))

# discard multiple occurrence of an editor name (different versions for instance)
def editor_count(df):
    """count the number of time an editor is used
    """
    return (df.groupby(['uid', 'fullname'])['num']
            .count()
            .reset_index()
            .groupby('fullname')['uid']
            .count()
            .sort_values(ascending=False))

def get_top_editor(editor_summary):
    """Get the top editors with extra info such as the ratio and cumsum

    editor_summary: dt
        result of the 'editor_count' function
    """
    data = (editor_summary.to_frame()
            .reset_index()
            .rename_axis({"uid": "num"}, axis=1))
    data['ratio'] = data['num'] / data['num'].sum() * 100.
    data['cumulative'] = data['ratio'].cumsum()
    return data

# some_trouble_names = ['Go Map!! 1.1', 'rosemary v0.3.11', 'Level0 v1',
#                       'ArcGIS Editor for OpenStreetMap (2.1)', 'OsmAnd~ 2.0.0#9942M',
#                       'QGIS OSM v0.5', 'OsmAnd+ 1.8.3']

# the problem to groupby fullname and sum 'num' is that you can have some rare
# editor which can carry out a large amount of modifications, i.e. changesets


# - top 10 or 20 of used editors
# - occurrence of used selected editors for each user (by top)

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 3:
        print("ERROR: need an input file and output file")
        sys.exit(0)

    INPUT_FNAME = sys.argv[1]
    # INPUT_FNAME = "/home/dag/data/osland-ia/soft-used-by-users.csv"
    OUTPUT_FNAME = sys.argv[2]
    # OUTPUT_FNAME = None
    df = main(INPUT_FNAME, OUTPUT_FNAME)
    top_editor = get_top_editor(editor_count(df))
    # after an analysis, we want to take the first 15th most used editors. The
    # other editors represent only less than 5% of the total used editors.
    selection = top_editor.fullname[:16].tolist() + ['other']

    # Set the 'other' label for editors which are not in the top selection
    logger.info("compute the number of 'other' editors")
    other_mask = np.logical_not(df['fullname'].isin(selection))
    df.loc[other_mask, 'fullname'] = 'other'
    logger.info("count the number of used selected editors by user")
    data = (df.groupby(["uid", "fullname"])["num"].sum()
            .unstack()
            .reset_index()
            .fillna(0))
    logger.info("Write the '%s' data file", OUTPUT_FNAME)
    data.to_csv(OUTPUT_FNAME, index=False)
