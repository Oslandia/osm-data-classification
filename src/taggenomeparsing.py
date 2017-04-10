# coding: utf-8

"""
Extract some stats for OSM nodes, from a history OSM data file

The file has to be runned through the following format:
python <relativepathto_OSM-history-parsing.py> <relativedatapath> <osmfile.pbf>
"""

import osmium as osm

class TagGenomeHandler(osm.SimpleHandler):

    def __init__(self):
        osm.SimpleHandler.__init__(self)
        self.taggenome = []

    def tag_inventory(self, elem, elem_type):
        for tag in elem.tags:
            self.taggenome.append([elem_type,
                                   elem.id,
                                   elem.version,
                                   tag.k,
                                   tag.v])

    def node(self, n):
        self.tag_inventory(n, "node")

    def way(self, w):
        self.tag_inventory(w, "way")

    def relation(self, r):
        self.tag_inventory(r, "relation")

