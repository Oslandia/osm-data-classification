# coding: utf-8

""" 
Plot some simple features of OSM history metadata (elements, change sets, users) 
How to run this script?
python <pathto_OSM-metadata-plotting.py> <pathto_data> <dataset_name>
"""

###############################################
# Import packages #############################
###############################################
import sys
import os.path as osp
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import utils

###############################################
# Main method #################################
###############################################
if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python <pathto_OSM-metadata-plotting.py> <pathto_data> <dataset_name>")
        sys.exit(-1)
    projectpath = osp.split(sys.argv[0])[0] + "/../"
    if projectpath == "/../":
        projectpath = "./../"
    datapath = sys.argv[1]
    ds_name = sys.argv[2]
    
    print("Plot OSM metadata corresponding to {0}".format(ds_name))
    # Data reading
    elemsynthesis, chgsetsynthesis, usersynthesis = utils.readOSMmd(datapath, ds_name)
    print(elemsynthesis.info())
    print(chgsetsynthesis.info())
    print(usersynthesis.info())

    
    ###############################################
    # Plot metadata: number of unique contributors per element
    print("Plot 1: number of contributors per element...")
    y = np.linspace(0,1,1001)
    utils.single_empCDF(elemsynthesis.n_user.quantile(y), y, lims=[0,20,0,1],
                        xlab="Number of unique contributors per OSM element",
                        ylab="Empirical CDF",
                        figpath=projectpath+"figs/"+ds_name+"-uniquserperelem.png")
    print("Plot 2: number of contributors per element and per type...")
    x = elemsynthesis.groupby('elem')['n_user'].quantile(y).unstack(0)
    utils.multi_empCDF(x, y, lims=[0,20,0,1], legend=True,
                       xlab="Number of unique contributors per OSM element",
                       ylab="Empirical CDF",
                       figpath=projectpath+"figs/"+ds_name+"-uniquserperelemtype.png")

    # Plot metadata: number of versions per element
    print("Plot 3: number of versions per element...")
    utils.single_empCDF(elemsynthesis.version.quantile(y), y, lims=[0,20,0,1],
                        xlab="Number of versions per OSM element",
                        ylab="Empirical CDF",
                        figpath=projectpath+"figs/"+ds_name+"-nbversionperelem.png")
    print("Plot 4: number of versions per element and per type...")
    x = elemsynthesis.groupby('elem')['version'].quantile(y).unstack(0)
    utils.multi_empCDF(x, y, lims=[0,80,0,1], legend=True,
                       xlab="Number of versions per OSM element",
                       ylab="Empirical CDF",
                       figpath=projectpath+"figs/"+ds_name+"-nbversionperelemtype.png")
    
    # Plot metadata: element global description
    print("Plot 5: element global description...")
    utils.condsctrplot('n_user', 'version', elemsynthesis.groupby('elem'),
                       lims=[0,150,0,600], bsctr=True, mkr="+", legend=True,
                       figpath=projectpath+"figs/"+ds_name+"-elem_uniquservsversion.png",
                       xlab="Number of unique users per OSM element",
                       ylab="Number of version per OSM element")

    # Plot metadata: number of modifications per change set
    print("Plot 6: number of contributors per element...")
    utils.single_empCDF(chgsetsynthesis.elem.quantile(y), y, lims=[0,200,0,1],
                        hltqt=0.9, ylab="Empirical CDF",
                        xlab="Number of modifications per change set",
                        figpath=projectpath+"figs/"+ds_name+"-nbmodifperchgset.png")
    print("Plot 7: number of contributors per element and per type...")
    x = (chgsetsynthesis.loc[:,['elem_node','elem_way','elem_relation']]
         .apply(lambda x: x.quantile(y)))
    utils.multi_empCDF(x, y, lims=[0,100,0,1], hltqt=0.9, legend=True,
                       xlab="Number of modifications per change set",
                       ylab="Empirical CDF",
                       figpath=projectpath+"figs/"+ds_name+"-nbmodifperchgsettype.png")
    
    # Plot metadata: change set global description
    print("Plot 8: change set global description...")
    x = chgsetsynthesis.loc[:,['elem_node','elem_way','elem_relation']]
    utils.multisctrplot(x, chgsetsynthesis.duration.astype('timedelta64[m]'),
                        lims=[0,np.log(max(x.apply(max))),0,700],
                        mkr="+", legend=True,
                        figpath=projectpath+"figs/"+ds_name+"-chgset_modifvsduration.png",
                        xlab="Logarithm of modification quantity per change sets",
                        ylab="Change set duration (in minutes)")

    # Plot metadata: number of change sets per user
    print("Plot 9: number of change set per user...")
    utils.single_empCDF(usersynthesis.n_chgset.quantile(y), y, lims=[0,100,0,1],
                        xlab="Number of change sets per user",
                        ylab="Empirical CDF",
                        figpath=projectpath+"figs/"+ds_name+"-nbchgsetperuser.png")
    
    # Plot metadata: number of modification per user
    print("Plot 10: number of modifications per user...")
    utils.single_empCDF(usersynthesis.n_modif.quantile(y), y,
                        lims=[0,100,0,1], hltqt=0.8,
                        xlab="Number of modifications per user",
                        ylab="Empirical CDF",
                        figpath=projectpath+"figs/"+ds_name+"-nbmodifperuser.png")
    print("Plot 11: number of modifications per user and per type...")
    x = (usersynthesis.loc[:,['n_nodemodif','n_waymodif','n_relationmodif']]
         .apply(lambda x: x.quantile(y)))
    utils.multi_empCDF(x, y, lims=[0,200,0,1], hltqt=0.8, legend=True,
                       xlab="Number of modifications per user",
                       ylab="Empirical CDF",
                       figpath=projectpath+"figs/"+ds_name+"-nbmodifperusertype.png")

    # Plot metadata: user global description
    print("Plot 12: user global description...")
    utils.scatterplot(usersynthesis.n_chgset,usersynthesis.n_modif, 
                      lims=[0,1000,0,10000], bsctr=True, mkr="s",
                      figpath=projectpath+"figs/"+ds_name+"-user_chgsetvsmodif.png",
                      xlab="Number of change sets by user",
                      ylab="Number of modifications per user")
    print("Plot 13: number of contributors per element...")
    utils.scatterplot(usersynthesis.n_chgset,
                      usersynthesis.activity.astype('timedelta64[D]'),
                      lims=[0,100,0,3500], bsctr=True, mkr="+", 
                      figpath=projectpath+"figs/"+ds_name+"-user_chgsetvsactivity.png",
                      xlab="Number of change sets by user",
                      ylab="User activity duration (in days)")
