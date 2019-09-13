# coding: utf-8

""" Extract some stats for OSM nodes, from a history OSM data file """

###############################################
# Import packages #############################
###############################################
import sys

import pandas as pd
import numpy as np

import utils

###############################################
# Main method #################################
###############################################
if __name__ == '__main__':
    # call the script following
    # format 'python <pathto_OSM-latest-data.py> <pathto_csvfiles> <dataset_name>' (3 args)
    if len(sys.argv) != 3:
        print("Usage: python3 <pathto_csvfiles> <dataset_name>")
        sys.exit(-1)
    datapath = sys.argv[1]
    dataset_name = sys.argv[2]

    # Data reading
    tlnodes, tlways, tlrelations = utils.readOSMdata(datapath, dataset_name)
    print("There are {0} feature(s) and {1} individual(s) in node dataframe"
          .format(len(tlnodes.columns), len(tlnodes)))
    print("There are {0} feature(s) and {1} individual(s) in way dataframe"
          .format(len(tlways.columns), len(tlways)))
    print("There are {0} feature(s) and {1} individual(s) in relation dataframe"
          .format(len(tlrelations.columns), len(tlrelations)))

    ###############################################
    # Build last OSM elements starting from history data
    # and save them into dedicated files
    nodes = utils.updatedelem(tlnodes)
    ways = utils.updatedelem(tlways)
    relations = utils.updatedelem(tlrelations)
    utils.writeOSMdata(nodes, ways, relations, datapath, dataset_name)
    
    
