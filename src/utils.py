# coding: utf-8

"""Some utility functions aiming to analyse OSM data
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

###############################################
# Utilities ###################################
###############################################
def readOSMdata(ds_name):
    """readOSMdata function: recover OSM elements from .csv files
    INPUT: ds_name = name of the data set (ex: bordeaux-metropole)
    OUTPUT: tlnodes, tlways, tlrelations = OSM element timelines, as dataframes
    """
    datapath = "~/data/" + ds_name + "/"
    tlnodes = pd.read_csv(datapath + "latest-" + ds_name + "-node-timeline.csv", index_col=0, parse_dates=['ts'])
    if(len(tlnodes.loc[tlnodes.visible==False]) == 0):
        # If 'visible' feature has not been recovered accurately
        # (each node is considered as visible...),
        # then recode the variable with coordinates
        tlnodes.loc[tlnodes.lon==float('inf'),'visible'] = False
    tlways = pd.read_csv(datapath + "latest-" + ds_name + "-way-timeline.csv", index_col=0, parse_dates=['ts'])
    if(len(tlways.loc[tlways.visible==False]) == 0):
        # If 'visible' feature has not been recovered accurately
        # (each node is considered as visible...),
        # then recode the variable with coordinates
        tlways.loc[tlways.nnodes==0,'visible'] = False
    tlrelations = pd.read_csv(datapath + "latest-" + ds_name + "-relation-timeline.csv", index_col=0, parse_dates=['ts'])
    if(len(tlrelations.loc[tlrelations.visible==False]) == 0):
        # If 'visible' feature has not been recovered accurately
        # (each node is considered as visible...),
        # then recode the variable with coordinates
        tlrelations.loc[tlrelations.nmembers==0,'visible'] = False
    return tlnodes, tlways, tlrelations

def updatedelem(data):
    """updatedelem function: return an updated version of OSM elements
    INPUT: data = OSM element timeline
    OUTPUT: updata = updated OSM elements
    """
    updata = data.groupby('id')['version'].max().reset_index()
    return pd.merge(updata,data,on=['id','version'])

def writeOSMdata(n, w, r, ds_name):
    """writeOSMdata function: write OSM elements into .csv files
    INPUT: n,w,r = OSM elements to write (resp. nodes, ways and relations); ds_name: name of the data set (ex: bordeaux-metropole)
    OUTPUT: 
    """
    datapath = "~/data/" + ds_name + "/"
    print("Writing OSM nodes into " + ds_name + "-nodes.csv file", end="...")
    n.to_csv(datapath + ds_name + "-nodes.csv")
    print("OK")
    print("Writing OSM ways into " + ds_name + "-ways.csv file", end="...")
    w.to_csv(datapath + ds_name + "-ways.csv")
    print("OK")
    print("Writing OSM relations into " + ds_name + "-relations.csv file", end="...")
    r.to_csv(datapath + ds_name + "-relations.csv")
    print("OK")

def writeOSMmetadata(e, cgs, u, ds_name):
    """writeOSMmetadata function: write OSM metadata into .csv files
    INPUT: e,cgs,u = OSM metadata to write (resp. elements, change sets and users); ds_name: name of the data set (ex: bordeaux-metropole)
    OUTPUT: 
    """
    datapath = "~/data/" + ds_name + "/"
    print("Writing element metadata into " + ds_name + "-md-elems.csv file", end="...")
    e.to_csv(datapath + ds_name + "-md-elems.csv")
    print("OK")
    print("Writing change set metadata into " + ds_name + "-md-chgsets.csv file", end="...")
    cgs.to_csv(datapath + ds_name + "-md-chgsets.csv")
    print("OK")
    print("Writing user metadata into " + ds_name + "-md-users.csv file", end="...")
    u.to_csv(datapath + ds_name + "-md-users.csv")
    print("OK")
    