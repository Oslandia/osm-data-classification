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

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import utils

###############################################
# Main method #################################
###############################################
if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python <pathto_OSM-tag-analyse.py> <pathto_csvfiles> <dataset_name>")
        sys.exit(-1)
    datapath = sys.argv[1]
    dataset_name = sys.argv[2]
    print("Recover OSM elements in " + datapath + "/"
          + dataset_name + "-elements.csv")
    osm_elements = pd.read_csv(datapath + "/" + dataset_name + "-elements.csv",
                               index_col=0, parse_dates=['ts'])


    ### Number of tags ###
    nbtags = osm_elements.ntags
    nbtags_n = osm_elements.query('elem == \"node\"').ntags
    nbtags_w = osm_elements.query('elem == \"way\"').ntags
    nbtags_r = osm_elements.query('elem == \"relation\"').ntags
    
    ### Tag genome ###
    
