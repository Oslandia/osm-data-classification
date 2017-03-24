# coding: utf-8

"""Extract some stats for OSM nodes, from a history OSM data file
The file has to be runned through the following format:                            python <relativepathto_node-history-stats.py> <relativepathto_osmfile.pbf> <relati\
vepathto_outputfile (optional)> 
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

########################################
class NodeTimelineHandler(osm.SimpleHandler):
    def __init__(self):
        osm.SimpleHandler.__init__(self)
        self.ids = set()
        # list of lists (node_id, lon, lat, version, visible, ts, uid, changesetid, ntags, tagkeys)
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
                                      len(n.tags),
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
                                      len(n.tags),
                                      [x.k for x in n.tags] ] )

########################################
# Main method        
if __name__ == '__main__':
    # call the script following format 'python3 node-stats.py <osmfile.pbf>' (2 args)
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Usage: python <pathto_OSM-history-parsing.py> <pathto_osmfile.pbf> (<pathto_output.csv>)")
        sys.exit(-1)
    filepath = sys.argv[1]
    if len(sys.argv) == 3:
        outputpath = sys.argv[2]
    else:
        outputpath = filepath                                
    
    # Recover the data through an instance of an osm handler
    nodehandler = NodeTimelineHandler()
    nodehandler.apply_file(filepath)
    print("Node number = {0}".format(len(nodehandler.nodetimeline)))

    # Convert handled nodes into a classic dataframe
    colnames = ['id', 'lon', 'lat', 'version', 'visible', 'ts', 'uid', 'chgset', 'ntags', 'tagkeys']
    nodes = pd.DataFrame(nodehandler.nodetimeline, columns=colnames)
      
   # order the columns
    nodes = nodes[colnames]
    nodes = nodes.sort_values(by=['id', 'ts'])
    
    # Write node data into a CSV file for further treatments
    output_filename_prefix = osp.splitext(outputpath)[0]
    if "." in osp.split(output_filename_prefix)[1] :
        output_filename_prefix = osp.splitext(osp.splitext(outputpath)[0])[0]
    output_filename = output_filename_prefix + "-tlnodes.csv"
    print("Write node history data into {0}".format(output_filename))
    nodes.to_csv(output_filename, date_format='%Y-%m-%d %H:%M:%S')
