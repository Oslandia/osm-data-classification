# coding: utf-8

"""Extract some stats for OSM change sets, from a history OSM data file
"""

import sys
import os.path as osp
from collections import defaultdict
from datetime import datetime, timezone
import pandas as pd
import osmium as osm

DEFAULT_END = pd.Timestamp("2000-01-01T00:00:00Z")
DEFAULT_START = pd.Timestamp("2018-01-01T00:00:00Z")

def chgsetdict():
    "make a default dict for each change set"
    return{"id":0,
           "first":DEFAULT_START,
           "last":DEFAULT_END,
           "uid":0,
           "nchanges":0,
           "nnodes":0,
           "nways":0,
           "nrelations":0
    }

class ChangesetHandler(osm.SimpleHandler):
    def __init__(self):
        osm.SimpleHandler.__init__(self)
        self.changesets = defaultdict(chgsetdict)

    def node(self, n):
        self.changesets[n.changeset]["id"] = n.changeset
        self.changesets[n.changeset]["nchanges"] += 1
        self.changesets[n.changeset]["nnodes"] += 1
        self.changesets[n.changeset]["uid"] = n.uid
        ts = pd.Timestamp(n.timestamp)
        self._update_last(n.changeset, ts)
        self._update_first(n.changeset, ts)

    def way(self, w):
        self.changesets[w.changeset]["id"] = w.changeset
        self.changesets[w.changeset]["nchanges"] += 1
        self.changesets[w.changeset]["nways"] += 1
        self.changesets[w.changeset]["uid"] = w.uid
        ts = pd.Timestamp(w.timestamp)
        self._update_last(w.changeset, ts)
        self._update_first(w.changeset, ts)

    def relation(self, r):
        self.changesets[r.changeset]["id"] = r.changeset
        self.changesets[r.changeset]["nchanges"] += 1
        self.changesets[r.changeset]["nrelations"] += 1
        self.changesets[r.changeset]["uid"] = r.uid
        ts = pd.Timestamp(r.timestamp)
        self._update_last(r.changeset, ts)
        self._update_first(r.changeset, ts)

    def _update_first(self, cs, ts):
        "update timestamp of the first change in the change set"
        if ts < self.changesets[cs]["first"]:
            self.changesets[cs]["first"] = ts

    def _update_last(self,cs, ts):
        "update timestamp of the last change in the change set"
        if ts > self.changesets[cs]["last"]:
            self.changesets[cs]["last"] = ts

# Main method        
if __name__ == '__main__':
    # call the script following format 'python3 changeset-stats.py <osmfile.pbf>' (2 args)
    if len(sys.argv) != 2:
        print("Usage: python3 changeset-stats.py <osmfile.pbf>")
        sys.exit(-1)
    filepath = sys.argv[1]

    # Recover the data through an instance of an osm handler
    chgsethandler = ChangesetHandler()
    chgsethandler.apply_file(filepath)
    print("Changeset number = {0}".format(len(chgsethandler.changesets)))

    # Convert handled change sets into a classic dataframe
    changesets = pd.DataFrame(chgsethandler.changesets).T
    #changesets['ts'] = changesets['ts'].apply(lambda x: x.tz_convert(None))

   # order the columns
    colnames = ['id', 'first', 'last', 'uid', 'nchanges', 'nnodes', 'nways', 'nrelations']
    changesets = changesets[colnames]
    changesets['first'] = changesets['first'].apply(lambda x: x.tz_convert(None))
    changesets['last'] = changesets['last'].apply(lambda x: x.tz_convert(None))

    # Write changesets data into a CSV file for further treatments
    output_filename = osp.splitext(filepath)[0]
    output_filename += "-chgset-timeline.csv"
    print("Write changeset data into {0}".format(output_filename))
    changesets.to_csv(output_filename, date_format='%Y-%m-%d %H:%M:%S')
