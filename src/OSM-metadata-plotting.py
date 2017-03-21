# coding: utf-8

""" Plot some simple features of OSM history metadata (elements, change sets, users) """

###############################################
# Import packages #############################
###############################################
import sys
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
    # format 'python3 OSM-metadata-plotting.py <data set name>' (2 args)
    if len(sys.argv) != 2:
        print("Usage: python3 OSM-metadata-plotting.py <dataset_name>")
        sys.exit(-1)
    dataset_name = sys.argv[1]
    print("Print OSM metadata corresponding to {0}".format(dataset_name))
    # Data reading
    elemsynthesis, chgsetsynthesis, usersynthesis = utils.readOSMmd(dataset_name)

    ###############################################
    # Plot metadata: number of unique contributors per element
    print("Plot 1: number of contributors per element...")
    y = np.linspace(0,1,1001)
    utils.single_empCDF(elemsynthesis.uid.quantile(y), y, lims=[0,20,0,1],
                        xlab="Number of unique contributors per OSM element",
                        ylab="Empirical CDF",
                        figpath="figs/uniquserperelem.png")
    print("Plot 2: number of contributors per element and per type...")
    x = elemsynthesis.groupby('elem')['uid'].quantile(y).unstack(0)
    utils.multi_empCDF(x, y, lims=[0,20,0,1], legend=True,
                       xlab="Number of unique contributors per OSM element",
                       ylab="Empirical CDF",
                       figpath="figs/uniquserperelemtype.png")

    # Plot metadata: number of versions per element
    print("Plot 3: number of versions per element...")
    utils.single_empCDF(elemsynthesis.version.quantile(y), y, lims=[0,20,0,1],
                        xlab="Number of versions per OSM element",
                        ylab="Empirical CDF",
                        figpath="figs/nbversionperelem.png")
    print("Plot 4: number of versions per element and per type...")
    x = elemsynthesis.groupby('elem')['version'].quantile(y).unstack(0)
    utils.multi_empCDF(x, y, lims=[0,80,0,1], legend=True,
                       xlab="Number of versions per OSM element",
                       ylab="Empirical CDF",
                       figpath="figs/nbversionperelemtype.png")

    # Plot metadata: element global description
    print("Plot 5: element global description...")
    utils.condsctrplot('uid', 'version', elemsynthesis.groupby('elem'),
                       lims=[0,150,0,600], bsctr=True, mkr="+", legend=True,
                       figpath="figs/elem_uniquservsversion.png",
                       xlab="Number of unique users per OSM element",
                       ylab="Number of version per OSM element")

    # Plot metadata: number of modifications per change set
    print("Plot 6: number of contributors per element...")
    utils.single_empCDF(chgsetsynthesis.elem.quantile(y), y, lims=[0,200,0,1],
                        hltqt=0.9, ylab="Empirical CDF",
                        xlab="Number of modifications per change set",
                        figpath="figs/nbmodifperchgset.png")
    print("Plot 7: number of contributors per element and per type...")
    x = chgsetsynthesis.loc[:,['nnode','nway','nrelation']].apply(
        lambda x: x.quantile(y))
    utils.multi_empCDF(x, y, lims=[0,100,0,1], hltqt=0.9, legend=True,
                       xlab="Number of modifications per change set",
                       ylab="Empirical CDF",
                       figpath="figs/nbmodifperchgsettype.png")
    
    # Plot metadata: change set global description
    print("Plot 8: change set global description...")
    x = chgsetsynthesis.loc[:,['nnode','nway','nrelation']]
    utils.multisctrplot(x, chgsetsynthesis.duration.astype('timedelta64[m]'),
                        lims=[0,np.log(max(x.apply(max))),0,700],
                        mkr="+", legend=True,
                        figpath="figs/chgset_modifvsduration.png",
                        xlab="Logarithm of modification quantity per change sets",
                        ylab="Change set duration (in minutes)")

    # Plot metadata: number of change sets per user
    print("Plot 9: number of change set per user...")
    utils.single_empCDF(usersynthesis.chgset.quantile(y), y, lims=[0,100,0,1],
                        xlab="Number of change sets per user",
                        ylab="Empirical CDF",
                        figpath="figs/nbchgsetperuser.png")
    
    # Plot metadata: number of modification per user
    print("Plot 10: number of modifications per user...")
    utils.single_empCDF(usersynthesis.elem.quantile(y), y,
                        lims=[0,100,0,1], hltqt=0.8,
                        xlab="Number of modifications per user",
                        ylab="Empirical CDF",
                        figpath="figs/nbmodifperuser.png")
    print("Plot 11: number of modifications per user and per type...")
    x = usersynthesis.loc[:,['nnode','nway','nrelation']].apply(
        lambda x: x.quantile(y))
    utils.multi_empCDF(x, y, lims=[0,200,0,1], hltqt=0.8, legend=True,
                       xlab="Number of modifications per user",
                       ylab="Empirical CDF",
                       figpath="figs/nbmodifperusertype.png")

    # Plot metadata: user global description
    print("Plot 12: user global description...")
    utils.scatterplot(usersynthesis.chgset,usersynthesis.elem, 
                      lims=[0,1000,0,10000], bsctr=True, mkr="s",
                      figpath="figs/user_chgsetvsmodif.png",
                      xlab="Number of change sets by user",
                      ylab="Number of modifications per user")
    print("Plot 13: number of contributors per element...")
    utils.scatterplot(usersynthesis.chgset,
                      usersynthesis.activity.astype('timedelta64[D]'),
                      lims=[0,100,0,3500], bsctr=True, mkr="+", 
                      figpath="figs/user_chgsetvsactivity.png",
                      xlab="Number of change sets by user",
                      ylab="User activity duration (in days)")
