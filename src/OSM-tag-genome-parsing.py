# coding: utf-8

"""
Extract some stats for OSM nodes, from a history OSM data file

The file has to be runned through the following format:
python <relativepathto_OSM-history-parsing.py> <relativedatapath> <osmfile.pbf>
"""

import sys
import os.path as osp
from collections import defaultdict
from datetime import datetime

import pandas as pd
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


if __name__ == '__main__':

    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Usage: python <pathto_OSM-tag-genome-parsing.py> <pathto_osmfile.pbf> (<pathto_outputdir.csv>)")
        sys.exit(-1)
    filepath = sys.argv[1]
    if len(sys.argv) == 3:
        outputpath = sys.argv[2]
    else:
        outputpath = filepath

    taghandler = TagGenomeHandler()
    taghandler.apply_file(filepath)
    print("There are {0} tag records in {1} dataset".format(len(taghandler.taggenome), osp.split(filepath)[1]))

    tag_genome = pd.DataFrame(taghandler.taggenome)
    tag_genome.columns = ['elem', 'id', 'version', 'tagkey', 'tagvalue']

    output_filename_prefix = osp.splitext(outputpath)[0]
    if "." in osp.split(output_filename_prefix)[1] :
        output_filename_prefix = osp.splitext(osp.splitext(outputpath)[0])[0]
    output_filename = output_filename_prefix + "-tag-genome.csv"
    print("Write OSM history data into {0}".format(output_filename))
    tag_genome.to_csv(output_filename)

