# coding: utf-8

"""Extract some stats for OSM ways, from a history OSM data file
The file has to be runned through the following format:                            
python <relativepathto_relation-history-stats.py> <relativepathto_osmfile.pbf> <relativepathto_outputfile (optional)>
"""

import sys
import os.path as osp
from collections import defaultdict
from datetime import datetime, timezone
import pandas as pd
import osmium as osm

DEFAULT_START = pd.Timestamp("2000-01-01T00:00:00Z")

class RelationTimelineHandler(osm.SimpleHandler):
    def __init__(self):
        osm.SimpleHandler.__init__(self)
        self.ids = set()
        # list of lists (way_id, version, visible, ts, uid, changesetid, nb_members, mids, nb_tags, tagkeys)
        self.relationtimeline = []
        
    def relation(self,r):
        self.ids.add(r.id)
        self.relationtimeline.append([r.id,
                                  r.version,
                                  r.visible,
                                  pd.Timestamp(r.timestamp),
                                  r.uid,
                                  r.changeset,
                                  len(r.members),
                                  [(m.ref,m.role,m.type) for m in r.members],
                                  len(r.tags),
                                  [x.k for x in r.tags] ] )

# Main method        
if __name__ == '__main__':
    # call the script following format 'python3 <pathto_relation-history-stats.py> <pathto_file.pbf> <pathto_output.csv (optional)>' (2(3) args) 
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Usage: python <pathto_relation-history-stats.py> <pathto_file.pbf> (<pathto_output.csv>)")
        sys.exit(-1)
    filepath = sys.argv[1]
    if len(sys.argv) == 3:
        outputpath = sys.argv[2]
    else:
        outputpath = filepath

    # Recover the data through an instance of an osm handler
    relationhandler = RelationTimelineHandler()
    relationhandler.apply_file(filepath)
    print("Relation number = {0}".format(len(relationhandler.relationtimeline)))

    # Convert handled relations into a classic dataframe
    colnames = ['id', 'version', 'visible', 'ts', 'uid', 'chgset',
                'nmembers', 'mids','ntags','tagkeys']
    relations = pd.DataFrame(relationhandler.relationtimeline, columns=colnames)

   # order the columns
    relations = relations[colnames]
    relations = relations.sort_values(by=['id', 'ts'])
    
    # Write relation data into a CSV file for further treatments
    output_filename_prefix = osp.splitext(filepath)[0]
    if "." in osp.split(output_filename_prefix)[1]:
        output_filename_prefix = osp.splitext(osp.splitext(outputpath)[0])[0]
    output_filename = output_filename_prefix + "-tlrelations.csv"
    print("Write relation data into {0}".format(output_filename))
    relations.to_csv(output_filename, date_format='%Y-%m-%d %H:%M:%S')
