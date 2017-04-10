# coding: utf-8

"""
Analyse the OSM tag genome, i.e. every tag keys associated to the OSM elements, from a history OSM data file
The file has to be runned through the following format:
python <pathto_OSM-tag-analyse.py> <pathto_csvfiles> <dataset_name>
"""

###############################################
# Import packages #############################
###############################################
import sys
import os.path as osp
from datetime import datetime
from collections import defaultdict

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import re


import utils

###############################################
# Utility functions ###########################
###############################################
def extract_tags_from_a_string(string):
    """
    extract_tags_from_a_string: extract OSM tag keys in a string with a dedicated format. It uses regular expressions
    INPUT: string = a character string in the format "['<tag1>', ..., '<tagn>']"
    OUTPUT: taglist = a list that contains tag keys (character strings)
    """
    return re.sub("[\[,\'\]]", "", string).split()

def extract_tags(string):
    """
    extract_tags: extract OSM tag keys from a string Series
    INPUT: string = a pandas Series that contains strings
    OUTPUT: taglist = a Series built with tag key lists, for each initial string
    """
    return string.apply(lambda x: extract_tags_from_a_string(x))

def build_tag_genome_from_series(tagkeys):
    """
    build_tag_genome: starting from tag key lists contained in a Series, build a global tag key list (list appending)
    INPUT: tagkeys = a pandas Series containing string lists
    OUTPUT: globaltaglist = list of every used tags in the OSM data history
    """    
    globaltaglist = []
    globaltaglist = [globaltaglist+line for line in tagkeys]
    globaltaglist = [val for sublist in globaltaglist for val in sublist]
    return set(globaltaglist)

def build_tag_genome(osmelem):
    """
    build_tag_genome: starting from an OSM history dataframe, build a global tag key list (list appending)
    INPUT: osmelem = a pandas dataframe containing OSM (history) data (with elem, id, version and tagkeys features)
    OUTPUT: tagstack = pandas Series of every used tags in the OSM data history
    """
    tagstack = osmelem.tagkeys.apply(pd.Series).stack().reset_index()
    tagstack.columns = ['index','secondaryindex','key']
    return tagstack[['index','key']]

def merge_tag_genome(osmelem):
    """
    merge_tag_genome: starting from an OSM history dataframe, build a global tag key list (list appending)
    INPUT: osmelem = a pandas dataframe containing OSM (history) data (with elem, id, version and tagkeys features)
    OUTPUT: globaltaglist = list of every used tags in the OSM data history
    """
    tagstack = osmelem.tagkeys.apply(pd.Series).stack()
    tagstack.name = 'key'
    tagstack = tagstack.reset_index()[['level_0','key']].set_index('level_0')
    return pd.merge(osmelem[['elem','id','version']],
                    tagstack,
                    left_index=True,
                    right_index=True)

###############################################
# Main method #################################
###############################################
if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python <pathto_OSM-tag-analyse.py> <pathto_csvfiles> <dataset_name>")
        sys.exit(-1)
    projectpath = osp.split(sys.argv[0])[0] + "/../"
    if projectpath == "/../":
        projectpath = "./../"
    datapath = sys.argv[1]
    dataset_name = sys.argv[2]
    print("Recover OSM elements in " + datapath + "/"
          + dataset_name + "-elements.csv")
    osm_elements = pd.read_csv(datapath + "/" + dataset_name + "-elements.csv",
                               index_col=0, parse_dates=['ts'])


    ### Number of tags ###
    y = np.linspace(0,1,1001)
    if(False):
        print("Plot 1: number of tags per element type...")
        x = osm_elements.groupby('elem')['ntags'].quantile(y).unstack(0)
        utils.multi_empCDF(x, y, lims=[0,100,0,1], legend=True,
                           xlab="Number of tags per OSM element",
                           ylab="Empirical CDF",
                           figpath=projectpath+"figs/"+dataset_name+"-tagpertype.png")

    nbtags = osm_elements.ntags
    nbtags_n = osm_elements.query('elem == \"node\"').ntags
    nbtags_w = osm_elements.query('elem == \"way\"').ntags
    nbtags_r = osm_elements.query('elem == \"relation\"').ntags

    ### Tag genome ###
    osm_elements['tagkeylists'] = extract_tags(osm_elements.tagkeys)
    tagkeys = build_tag_genome_from_series(osm_elements.tagkeylists)
    tagcount = [sum(k in keys for keys in osm_elements.tagkeys)
                for k in list(tagkeylists)]
    nodetagcount = [sum(k in keys
                    for keys in osm_elements.query("elem==\"node\"").tagkeys)
                for k in list(tagkeylists)]
    waytagcount = [sum(k in keys
                    for keys in osm_elements.query("elem==\"node\"").tagkeys)
                for k in list(tagkeylists)]
    relationtagcount = [sum(k in keys
                    for keys in osm_elements.query("elem==\"node\"").tagkeys)
                for k in list(tagkeylists)]
    tagcount = pd.DataFrame({'key': tagkeylists,
                             'tagcount': tagcount,
                             'nodetagcount': nodetagcount,
                             'waytagcount': waytagcount,
                             'relationtagcount': relationtagcount})
    tagcount = tagcount.sort_values('key')
    
   # taggenome = build_tag_genome(osm_elements)
    
