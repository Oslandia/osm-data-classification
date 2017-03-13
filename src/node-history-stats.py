# coding: utf-8

"""Extract some stats for OSM nodes, from a history OSM data file
"""

import sys
import os.path as osp
from collections import defaultdict
from datetime import datetime, timezone
#if sys.version_info[0] == 3:
#    from datetime import timezone
import pandas as pd
import osmium as osm

DEFAULT_START = pd.Timestamp("2000-01-01T00:00:00Z")

class NodeTimelineHandler(osm.SimpleHandler):
    def __init__(self):
        osm.SimpleHandler.__init__(self)
        self.ids = set()
        # list of lists (node_id, lon, lat, version, visible, ts, uid, changesetid, tagkeys)
        self.nodetimeline = []
        
    def node(self,n):
        self.ids.add(n.id)
        nodeloc = n.location
        if nodeloc.valid():
            self.nodetimeline.append([n.id,
                                      nodeloc.lon,
                                      nodeloc.lat,
                                      n.version,
                                      not(n.deleted),
                                      pd.Timestamp(n.timestamp),
                                      n.uid,
                                      n.changeset,
                                      [x.k for x in n.tags] ] )
        else:
            self.nodetimeline.append([n.id,
                                      float('inf'),
                                      float('inf'),
                                      n.version,
                                      not(n.deleted),
                                      pd.Timestamp(n.timestamp),
                                      n.uid,
                                      n.changeset,
                                      [x.k for x in n.tags] ] )


# Main method        
if __name__ == '__main__':
    # call the script following format 'python3 node-stats.py <osmfile.pbf>' (2 args)
    if len(sys.argv) != 2:
        print("Usage: python3 node-stats.py <osmfile.pbf>")
        sys.exit(-1)
    filepath = sys.argv[1]

    # Recover the data through an instance of an osm handler
    nodehandler = NodeTimelineHandler()
    nodehandler.apply_file(filepath)
    print("Node number = {0}".format(len(nodehandler.nodetimeline)))

    # Convert handled nodes into a classic dataframe
    colnames = ['id', 'lon', 'lat', 'version', 'visible', 'ts', 'uid', 'chgset', 'tagkeys']
    nodes = pd.DataFrame(nodehandler.nodetimeline, columns=colnames)
   # nodes['ts'] = nodes['ts'].apply(lambda x: x.tz_convert(None))

   # order the columns
    nodes = nodes[colnames]
    nodes = nodes.sort_values(by=['id', 'ts'])
    
    # Write node data into a CSV file for further treatments
    output_filename = osp.splitext(filepath)[0]
    output_filename += "-node-timeline.csv"
    print("Write node data into {0}".format(output_filename))
    nodes.to_csv(output_filename, date_format='%Y-%m-%d %H:%M:%S')
