# coding: utf-8

"""
Extract some stats for OSM nodes, from a history OSM data file The
file has to be runned through the following format:
python <pathto_OSM-history-parsing.py> <pathto_osmfile.pbf> <pathto_outputdir (optional)>
"""

import sys
import os.path as osp
from collections import defaultdict
from datetime import datetime
if sys.version_info[0] == 3:
    from datetime import timezone
import pandas as pd
import osmium as osm

########################################
DEFAULT_START = pd.Timestamp("2000-01-01T00:00:00Z")

class TimelineHandler(osm.SimpleHandler):
    """Encapsulates the recovery of elements inside the OSM history.

    This history is composed of nodes, ways and relations, that have
    common attributes (id, version, visible, timestamp, userid,
    changesetid, nbtags, tagkeys). Nodes are characterized with their
    latitude and longitude; ways with the nodes that composed them;
    relations with a list of OSM elements that compose it, aka their
    members (a member can be a node, a way or another relation). We
    gather these informations into a single attributes named
    'descr'. The timeline handler consider the OSM element type as
    well (node, way or relation). OSM history dumps do not seem to
    update properly 'visible' flag of OSM elements, so this handler
    recode it according to elements property.

    """
    def __init__(self):
        """ Class default constructor"""
        osm.SimpleHandler.__init__(self)
        self.elemtimeline = [] # Dictionnary of OSM elements
        
    def node(self,n):
        """
        Node recovery: each record in the history is saved as a row in the
        element dataframe.
        
        The features are the following: id, version, visible?,
        timestamp, userid, chgsetid, nbtags, tagkeys, elem type
        ("node") and geographical coordinates (lat, lon)

        """
        nodeloc = n.location
        # If the location is not valid, then the node is no longer available
        if nodeloc.valid():
            self.elemtimeline.append([n.id,
                                      n.version,
                                      True, # 'visible' flag not OK
                                      pd.Timestamp(n.timestamp),
                                      n.uid,
                                      n.changeset,
                                      len(n.tags),
                                      [x.k for x in n.tags],
                                      "node",
                                      (nodeloc.lat, nodeloc.lon)])
        else:
            self.elemtimeline.append([n.id,
                                      n.version,
                                      False,
                                      pd.Timestamp(n.timestamp),
                                      n.uid,
                                      n.changeset,
                                      len(n.tags),
                                      [x.k for x in n.tags],
                                      "node",
                                      (float('nan'), float('nan'))])


    def way(self,w):
        """
        Way recovery: each record in the history is saved as a row in the
        element dataframe.

        The features are the following: id,
        version, visible?, timestamp, userid, chgsetid, nbtags,
        tagkeys, elem type ("way") and a tuple (node quantity, list of
        nodes)
        """
        # If there is no nodes in the way, then the way is no longer available
        if len(w.nodes) > 0 :
            self.elemtimeline.append([w.id,
                                      w.version,
                                      True, # 'visible' flag not OK
                                      pd.Timestamp(w.timestamp),
                                      w.uid,
                                      w.changeset,
                                      len(w.tags),
                                      [x.k for x in w.tags],
                                      "way",
                                      (len(w.nodes), [n.ref for n in w.nodes])])
        else:
            self.elemtimeline.append([w.id,
                                      w.version,
                                      False,
                                      pd.Timestamp(w.timestamp),
                                      w.uid,
                                      w.changeset,
                                      len(w.tags),
                                      [x.k for x in w.tags],
                                      "way",
                                      (len(w.nodes), [n.ref for n in w.nodes])])

                                     
    def relation(self,r):
        """
        Relation recovery: each record in the history is saved as a row in
        the element dataframe.

        The features are the following: id,
        version, visible?, timestamp, userid, chgsetid, nbtags,
        tagkeys, elem type ("relation") and a tuple (member quantity,
        list of members under format (id, role, type))
        """
        # If the relation does not include any member, it is no longer available
        if len(r.members) > 0 or len(r.tags) > 0 :
            self.elemtimeline.append([r.id,
                                      r.version,
                                      True, # 'visible' flag not OK
                                      pd.Timestamp(r.timestamp),
                                      r.uid,
                                      r.changeset,
                                      len(r.tags),
                                      [x.k for x in r.tags],
                                      "relation",
                                      (len(r.members), [(m.ref,m.role,m.type)
                                                        for m in r.members])])
        else:
            self.elemtimeline.append([r.id,
                                      r.version,
                                      False,
                                      pd.Timestamp(r.timestamp),
                                      r.uid,
                                      r.changeset,
                                      len(r.tags),
                                      [x.k for x in r.tags],
                                      "relation",
                                      (len(r.members), [(m.ref,m.role,m.type)
                                                        for m in r.members])])

########################################
# Main method        
if __name__ == '__main__':
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Usage: python <pathto_OSM-history-parsing.py> <pathto_osmfile.pbf> (<pathto_outputdir.csv>)")
        sys.exit(-1)
    filepath = sys.argv[1]
    if len(sys.argv) == 3:
        outputpath = sys.argv[2]
    else:
        outputpath = filepath

    # Recover the data through an instance of an osm handler
    tlhandler = TimelineHandler()
    tlhandler.apply_file(filepath)
    print("Element number = {0}".format(len(tlhandler.elemtimeline)))

    # Convert handled nodes into a classic dataframe
    colnames = ['id', 'version', 'visible', 'ts', 'uid',
                'chgset', 'ntags', 'tagkeys', 'elem', 'descr']
    elements = pd.DataFrame(tlhandler.elemtimeline, columns=colnames)
    elements = elements.sort_values(by=['elem', 'id', 'version'])
    
    # Write node data into a CSV file for further treatments
    output_filename_prefix = osp.splitext(outputpath)[0]
    if "." in osp.split(output_filename_prefix)[1] :
        output_filename_prefix = osp.splitext(osp.splitext(outputpath)[0])[0]
    output_filename = output_filename_prefix + "-elements.csv"
    print("Write OSM history data into {0}".format(output_filename))
    elements.to_csv(output_filename, date_format='%Y-%m-%d %H:%M:%S')
