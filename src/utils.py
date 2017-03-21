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
import numpy as np
import matplotlib.pyplot as plt

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

def readOSMmd(ds_name):
    """readOSMmd function: recover OSM metadata from three .csv files (on the disk after OSM-metadata-extract.py module running)
    INPUT: ds_name = name of the data set (ex: bordeaux-metropole)
    OUTPUT: elemsynthesis,chgsetsynthesis,usersynthesis = OSM elements, change sets and users
    """
    datapath = "~/data/" + ds_name + "/" + ds_name
    elems = pd.read_csv(datapath+"-md-elems.csv", index_col=0, parse_dates=['ts'])
    chgsets = pd.read_csv(datapath+"-md-chgsets.csv", index_col=0, parse_dates=['ts'])
    users = pd.read_csv(datapath+"-md-users.csv", index_col=0, parse_dates=['ts'])
    return elems, chgsets, users

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

def single_empCDF(x, y, lims=False, xlab=False, ylab=False, title=False, legend=False, figpath=False, hltqt=False):
    """single_empCDF function: plot empirical cumulative distribution function of variable Y through quantile function (one single curve)
    INPUT: x: numpy array x-axis (cf linspace); y: empirical CDF; xlab, ylab: axis labels; xlim, ylim: axis limits; title: graph title; legend: legend of the plot
    """
    osmplot = plt.plot(x, y, linewidth=2)
    if xlab != False:
        plt.xlabel(xlab)
    if ylab != False:
        plt.ylabel(ylab)
    if title != False:
        plt.title(title)
    if lims != False:
        plt.axis(lims)
        plt.xticks(np.arange(lims[0], lims[1]+0.1, (lims[1]-lims[0])/10.0))
        plt.yticks(np.arange(lims[2], lims[3]+0.1, (lims[3]-lims[2])/10.0))
    if hltqt != False:
        plt.axhline(hltqt, color="grey", linestyle="dotted")
        plt.axvline(x.loc[hltqt], color="grey", linestyle="dotted")
    if legend != False:
        plt.legend(osmplot, ['nodes','ways','relations'], loc="lower right")
    if figpath != False:
        plt.savefig(figpath)
    plt.close("all")

def multi_empCDF(x, y, lims=False, xlab=False, ylab=False, title=False, legend=False, figpath=False, hltqt=False):
    """multi_empCDF function: plot empirical cumulative distribution functions of variable Y through quantile functions (multiplot)
    INPUT: x: numpy array x-axis (cf linspace); y: empirical CDF; xlab, ylab: axis labels; xlim, ylim: axis limits; title: graph title; legend: legend of the plot
    """
    [nplot, wplot, rplot] = plt.plot(x, y, linewidth=2)
    if xlab != False:
        plt.xlabel(xlab)
    if ylab != False:
        plt.ylabel(ylab)
    if title != False:
        plt.title(title)
    if lims != False:
        plt.axis(lims)
        plt.xticks(np.arange(lims[0], lims[1]+0.1, (lims[1]-lims[0])/10.0))
        plt.yticks(np.arange(lims[2], lims[3]+0.1, (lims[3]-lims[2])/10.0))
    if hltqt != False:
        plt.axhline(hltqt, color="grey", linestyle="dotted")
        [plt.axvline(h, color="grey", linestyle="dotted") for h in x.loc[hltqt,].values]
    if legend != False:
        plt.legend([nplot,wplot,rplot], ['nodes','ways','relations'], loc="lower right")
    if figpath != False:
        plt.savefig(figpath)
    plt.close("all")

def scatterplot(x, y, lims=False, xlab=False, ylab=False, title=False, figpath=False, bsctr=False, mkr=".", col="b"):
    """scatterplot function: scatter plot between variables x and y
    INPUT: x: numpy array x-axis (cf linspace); y: empirical CDF; xlab, ylab: axis labels; xlim, ylim: axis limits; title: graph title; legend: legend of the plot
    """
    plt.scatter(x,y, color=col, marker=mkr)
    if xlab != False:
        plt.xlabel(xlab)
    if ylab != False:
        plt.ylabel(ylab)
    if title != False:
        plt.title(title)
    if lims != False:
        plt.axis(lims)
        plt.xticks(np.arange(lims[0], lims[1]+0.1, (lims[1]-lims[0])/10.0))
        plt.yticks(np.arange(lims[2], lims[3]+0.1, (lims[3]-lims[2])/10.0))
    if bsctr != False:
        plt.plot(np.linspace(0,lims[1],lims[1]), color="grey", linestyle="dotted")
    if figpath != False:
        plt.savefig(figpath)
    plt.close("all")

def condsctrplot(xname, yname, factor, lims=False, xlab=False, ylab=False, title=False, legend=False, figpath=False, bsctr=False, mkr="."):
    """condsctrplot function: conditional scatter plot between xname and yname, according to factor (the category of each individual)
    INPUT: x: numpy array x-axis (cf linspace); y: empirical CDF; xlab, ylab: axis labels; xlim, ylim: axis limits; title: graph title; legend: legend of the plot
    """
    for key, group in factor:
        plt.plot(group[xname], group[yname], linestyle='',
                 marker=mkr, label=key,
                 color={'n':"blue",'w':"green",'r':"red"}[key])
    if xlab != False:
        plt.xlabel(xlab)
    if ylab != False:
        plt.ylabel(ylab)
    if title != False:
        plt.title(title)
    if lims != False:
        plt.axis(lims)
        plt.xticks(np.arange(lims[0], lims[1]+0.1, (lims[1]-lims[0])/10.0))
        plt.yticks(np.arange(lims[2], lims[3]+0.1, (lims[3]-lims[2])/10.0))
    if bsctr != False:
        plt.plot(np.linspace(0,lims[1],lims[1]), color="grey", linestyle="dotted")
    if legend != False:
        plt.legend(numpoints=1, loc="upper right")
    if figpath != False:
        plt.savefig(figpath)
    plt.close("all")

def multisctrplot(x, y, lims=False, xlab=False, ylab=False, title=False, legend=False, figpath=False, mkr="."):
    """multisctrplot function: scatter plot of several series (x being a dataframe)
    INPUT: x: pandas DataFrame with nnode, nway and nrelation features; y: empirical CDF; xlab, ylab: axis labels; lims: axis limits with format[xmin, xmax, ymin, ymax]; title: graph title; legend: legend of the plot, mkr: point marker
    """
    nsctr = plt.scatter(np.log(x['nnode']), y, color='b', marker=mkr)
    wsctr = plt.scatter(np.log(x['nway']), y, color='g', marker=mkr)
    rsctr = plt.scatter(np.log(x['nrelation']), y, color='r', marker=mkr)
    if xlab != False:
        plt.xlabel(xlab)
    if ylab != False:
        plt.ylabel(ylab)
    if title != False:
        plt.title(title)
    if lims != False:
        plt.axis(lims)
        #plt.xticks(np.arange(lims[0], lims[1]+0.1, (lims[1]-lims[0])/10.0))
        plt.yticks(np.arange(lims[2], lims[3]+0.1, (lims[3]-lims[2])/10.0))
    if legend != False:
        plt.legend([nsctr,wsctr,rsctr], ["nodes","ways","relations"], numpoints=1, loc="upper right")
    if figpath != False:
        plt.savefig(figpath)
    plt.close("all")
