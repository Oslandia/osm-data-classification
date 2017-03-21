# coding: utf-8

"""Extract some stats for OSM nodes, from a history OSM data file
"""

###############################################
# Import packages #############################
###############################################
import sys
import os.path as osp
from datetime import datetime
if sys.version_info[0] == 3:
    from datetime import timezone

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import utils

###############################################
# Main method #################################
###############################################
if __name__ == '__main__':
    # call the script following
    # format 'python3 <data set name> <save_output?>' (2 args)
    if len(sys.argv) != 3:
        print("Usage: python3 <dataset_name> <save_output: y/n>")
        sys.exit(-1)
    dataset_name = sys.argv[1]
    save_output = True if sys.argv[2]=="y" or sys.argv[2]=="Y" else False
    print("Analyse of the OSM data gathered for {0}".format(dataset_name))
    if(save_output):
        print("Outputs will be saved into .csv files during process!")
    # Data reading
    tlnodes, tlways, tlrelations = utils.readOSMdata(dataset_name)
    print("There are {0} feature(s) and {1} individual(s) in node dataframe"
          .format(len(tlnodes.columns),len(tlnodes)))
    print("There are {0} feature(s) and {1} individual(s) in way dataframe"
          .format(len(tlways.columns),len(tlways)))
    print("There are {0} feature(s) and {1} individual(s) in relation dataframe"
          .format(len(tlrelations.columns),len(tlrelations)))

    ###############################################
    # Build last OSM elements starting from history data
    # and save them into dedicated files
    nodes = utils.updatedelem(tlnodes)
    ways = utils.updatedelem(tlways)
    relations = utils.updatedelem(tlrelations)
    if(save_output):
        utils.writeOSMdata(nodes,ways,relations,dataset_name)
    
    
