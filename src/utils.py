# coding: utf-8

"""
Some utility functions aiming to analyse OSM data
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

### I/O utilities #############################
def readOSMdata(datapath, ds_name):
    """
    readOSMdata function: recover OSM elements from .csv files
    INPUT: datapath = relative path to directory that contains OSM data .csv files; ds_name: name of the data set (ex: bordeaux-metropole)
    OUTPUT: tlnodes, tlways, tlrelations = OSM element timelines, as dataframes
    """
    tlnodes = pd.read_csv(datapath+"/"+ds_name+"-tlnodes.csv",
                          index_col=0, parse_dates=['ts'])
    if(len(tlnodes.loc[tlnodes.visible==False]) == 0):
        # If 'visible' feature has not been recovered accurately
        # (each node is considered as visible...),
        # then recode the variable with coordinates
        tlnodes.loc[tlnodes.lon==float('inf'),'visible'] = False
    tlways = pd.read_csv(datapath + "/" + ds_name + "-tlways.csv",
                         index_col=0, parse_dates=['ts'])
    if(len(tlways.loc[tlways.visible==False]) == 0):
        # If 'visible' feature has not been recovered accurately
        # (each node is considered as visible...),
        # then recode the variable with coordinates
        tlways.loc[tlways.nnodes==0, 'visible'] = False
    tlrelations = pd.read_csv(datapath + "/" + ds_name + "-tlrelations.csv",
                              index_col=0, parse_dates=['ts'])
    if(len(tlrelations.loc[tlrelations.visible==False]) == 0):
        # If 'visible' feature has not been recovered accurately
        # (each node is considered as visible...),
        # then recode the variable with coordinates
        tlrelations.loc[np.logical_or(tlrelations.nmembers==0,tlrelations.ntags==0)
                        , 'visible'] = False
    return tlnodes, tlways, tlrelations

def readOSMmd(datapath, ds_name):
    """readOSMmd function: recover OSM metadata from three .csv files (on the disk after OSM-metadata-extract.py module running)
    INPUT: datapath: relative path to data files; ds_name: name of the data set (ex: bordeaux-metropole)
    OUTPUT: elemsynthesis,chgsetsynthesis,usersynthesis = OSM elements, change sets and users
    """
    elems = pd.read_csv(datapath+"/"+ds_name+"-md-elems.csv",
                          index_col=0, parse_dates=['created_at','lastmodif_at'])
    elems.lifecycle = pd.to_timedelta(elems.lifecycle)
    chgsets = pd.read_csv(datapath+"/"+ds_name+"-md-chgsets.csv",
                          index_col=0, parse_dates=['opened_at','lastmodif_at'])
    chgsets.duration = pd.to_timedelta(chgsets.duration)
    users = pd.read_csv(datapath+"/"+ds_name+"-md-users.csv",
                        index_col=0, parse_dates=['first_at','last_at'])
    users.activity = pd.to_timedelta(users.activity)
    return elems, chgsets, users

def writeOSMdata(n, w, r, datapath, ds_name):
    """writeOSMdata function: write OSM elements into .csv files
    INPUT: n,w,r = OSM elements to write (resp. nodes, ways and relations); datapath: relative path to the data files (repository in which data will be saved); ds_name: name of the data set (ex: bordeaux-metropole)
    OUTPUT: 
    """
    print("Writing OSM nodes into " + datapath + "/" + ds_name + "-nodes.csv file", end="...")
    n.to_csv(datapath + "/" + ds_name + "-nodes.csv")
    print("OK")
    print("Writing OSM ways into " + datapath + "/" + ds_name + "-ways.csv file", end="...")
    w.to_csv(datapath + "/" + ds_name + "-ways.csv")
    print("OK")
    print("Writing OSM relations into " + datapath + "/" + ds_name + "-relations.csv file", end="...")
    r.to_csv(datapath + "/" + ds_name + "-relations.csv")
    print("OK")

def writeOSMmetadata(e, cgs, u, datapath, ds_name):
    """writeOSMmetadata function: write OSM metadata into .csv files
    INPUT: e,cgs,u = OSM metadata to write (resp. elements, change sets and users); datapath: relative path to the data files (repository in which data will be saved); ds_name: name of the data set (ex: bordeaux-metropole)
    OUTPUT: 
    """
    print("Writing element metadata into " + datapath + "/" + ds_name + "-md-elems.csv file", end="...")
    e.to_csv(datapath + "/" + ds_name + "-md-elems.csv")
    print("OK")
    print("Writing change set metadata into " + datapath + "/" + ds_name + "-md-chgsets.csv file", end="...")
    cgs.to_csv(datapath + "/" + ds_name + "-md-chgsets.csv")
    print("OK")
    print("Writing user metadata into " + datapath + "/" + ds_name + "-md-users.csv file", end="...")
    u.to_csv(datapath + "/" + ds_name + "-md-users.csv")
    print("OK")

### Plotting utilities ########################
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
                 color={'node':"blue",'way':"green",'relation':"red"}[key])
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
    nsctr = plt.scatter(np.log(x['elem_node']), y, color='b', marker=mkr)
    wsctr = plt.scatter(np.log(x['elem_way']), y, color='g', marker=mkr)
    rsctr = plt.scatter(np.log(x['elem_relation']), y, color='r', marker=mkr)
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

### OSM data exploration ######################
def updatedelem(data):
    """updatedelem function: return an updated version of OSM elements
    INPUT: data = OSM element timeline
    OUTPUT: updata = updated OSM elements
    """
    updata = data.groupby(['elem','id'])['version'].max().reset_index()
    return pd.merge(updata, data, on=['id','version'])

def datedelems(history, date):
    """
    datedelems: return an updated version of history data at date
    INPUT: history = OSM history dataframe, date = date in datetime format
    OUTPUT: datedelems = dataframe with up-to-date elements (at date 'date')
    """
    datedelems = (history.query("ts <= @date")
                  .groupby(['elem','id'])['version']
                  .max()
                  .reset_index())
    return pd.merge(datedelems, history, on=['elem','id','version'])

### OSM metadata extraction ####################
def groupuser_count(metadata, data, grp_feat, res_feat, namesuffix):
    """Group-by 'data' by 'grp_feat' and element type features, count element
    corresponding to each grp_feat-elemtype tuples and merge them into metadata
    table

    INPUT: metadata = df that is modified during processing, data = df from
    where information is grouped, grp_feat = string that indicates which feature
    from 'data' must be used to group items, res_feat = string that indicates the
    measured feature (how many items correspond to the criterion), namesuffix =
    end of strings that represents the corresponding features

    """
    md_ext = (data.groupby([grp_feat, 'elem'])[res_feat]
              .count()
              .unstack()
              .reset_index()
              .fillna(0))
    md_ext['elem'] = md_ext[['node','relation','way']].apply(sum, axis=1)
    colnames = "n_" + md_ext.columns.values[-4:] + namesuffix
    md_ext.columns = [grp_feat, *colnames]
    return pd.merge(metadata, md_ext, on=grp_feat, how='outer').fillna(0)

def groupuser_nunique(metadata, data, grp_feat, res_feat, namesuffix):
    """Group-by 'data' by 'grp_feat' and element type features, count unique
    element corresponding to each grp_feat-elemtype tuples and merge them into
    metadata table

    INPUT: metadata = df that is modified during processing, data = df from
    where information is grouped, grp_feat = string that indicates which feature
    from 'data' must be used to group items, res_feat = string that indicates the
    measured feature (how many items correspond to the criterion), namesuffix =
    end of strings that represents the corresponding features

    """
    md_ext = (data.groupby([grp_feat, 'elem'])[res_feat]
              .nunique()
              .unstack()
              .reset_index()
              .fillna(0))
    md_ext['elem'] = md_ext[['node','relation','way']].apply(sum, axis=1)
    colnames = "n_" + md_ext.columns.values[-4:] + namesuffix
    md_ext.columns = [grp_feat, *colnames]
    return pd.merge(metadata, md_ext, on=grp_feat, how='outer').fillna(0)


def groupuser_stats(metadata, data, grp_feat, res_feat, namesuffix):
    """Group-by 'data' by 'grp_feat' and element type features, compute basic
    statistic features (min, median, max) corresponding to each grp_feat-elemtype
    tuples and merge them into metadata table

    INPUT: metadata = df that is modified during processing, data = df from
    where information is grouped, grp_feat = string that indicates which feature
    from 'data' must be used to group items, res_feat = string that indicates the
    measured feature (how many items correspond to the criterion), namesuffix =
    end of strings that represents the corresponding features

    """
    md_ext = (data.groupby(grp_feat)[res_feat].agg({'min': "min",
                                              'med': "median",
                                              'max': "max"}).reset_index())
    colnames = ["n" + op + "_" + namesuffix for op in md_ext.columns.values[1:]]
    md_ext.columns = [grp_feat, *colnames]
    metadata = pd.merge(metadata, md_ext, on=grp_feat, how='outer').fillna(0)
    return metadata
