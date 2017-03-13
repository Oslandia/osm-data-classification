# coding: utf-8

"""Extract some stats for OSM ways, from a history OSM data file
"""

import sys
import os.path as osp
from collections import defaultdict
from datetime import datetime, timezone
import pandas as pd
import osmium as osm

DEFAULT_START = pd.Timestamp("2000-01-01T00:00:00Z")

class WayTimelineHandler(osm.SimpleHandler):
    def __init__(self):
        osm.SimpleHandler.__init__(self)
        self.ids = set()
        # list of lists (way_id, version, visible, ts, uid, changesetid, nb_nodes, nids, nb_tags, tagkeys)
        self.waytimeline = []
        
    def way(self,w):
        self.ids.add(w.id)
        self.waytimeline.append([w.id,
                                  w.version,
                                  w.visible,
                                  pd.Timestamp(w.timestamp),
                                  w.uid,
                                  w.changeset,
                                  len(w.nodes),
                                  [n.ref for n in w.nodes],
                                  len(w.tags),
                                  [x.k for x in w.tags] ] )

# Main method        
if __name__ == '__main__':
    # call the script following format 'python3 node-stats.py <osmfile.pbf>' (2 args)
    if len(sys.argv) != 2:
        print("Usage: python3 node-stats.py <osmfile.pbf>")
        sys.exit(-1)
    filepath = sys.argv[1]

    # Recover the data through an instance of an osm handler
    wayhandler = WayTimelineHandler()
    wayhandler.apply_file(filepath)
    print("Way number = {0}".format(len(wayhandler.waytimeline)))

    # Convert handled nodes into a classic dataframe
    colnames = ['id', 'version', 'visible', 'ts', 'uid', 'chgset', 'nnodes', 'nids', 'ntags', 'tagkeys']
    ways = pd.DataFrame(wayhandler.waytimeline, columns=colnames)
    #ways['ts'] = ways['ts'].apply(lambda x: x.tz_convert(None))

   # order the columns
    ways = ways[colnames]
    ways = ways.sort_values(by=['id', 'ts'])
    
    # Write node data into a CSV file for further treatments
    output_filename = osp.splitext(filepath)[0]
    output_filename += "-way-timeline.csv"
    print("Write way data into {0}".format(output_filename))
    ways.to_csv(output_filename, date_format='%Y-%m-%d %H:%M:%S')
