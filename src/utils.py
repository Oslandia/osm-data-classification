# !/usr/bin/env python
# coding: utf-8

"""
Some utility functions aiming to analyse OSM data
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

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
    """Return an updated version of OSM elements

    Parameters
    ----------
    data: df
        OSM element timeline
    
    """
    updata = data.groupby(['elem','id'])['version'].max().reset_index()
    return pd.merge(updata, data, on=['id','version'])

def datedelems(history, date):
    """Return an updated version of history data at date

    Parameters
    ----------
    history: df
        OSM history dataframe
    date: datetime
        date in datetime format

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

    Parameters
    ----------
    metadata: df
        Dataframe that will integrate the new features
    data: df
        Dataframe from where information is grouped
    grp_feat: object
        string that indicates which feature from 'data' must be used to group items
    res_feat: object
        string that indicates the measured feature (how many items correspond
    to the criterion)
    namesuffix: object
        string that ends the new feature name
    
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

    Parameters
    ----------
    metadata: df
        Dataframe that will integrate the new features
    data: df
        Dataframe from where information is grouped
    grp_feat: object
        string that indicates which feature from 'data' must be used to group items
    res_feat: object
        string that indicates the measured feature (how many items correspond
    to the criterion)
    namesuffix: object
        string that ends the new feature name

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
    statistic features (min, median, max) corresponding to each
    grp_feat-elemtype tuples and merge them into metadata table

    Parameters
    ----------
    metadata: df
        Dataframe that will integrate the new features
    data: df
        Dataframe from where information is grouped
    grp_feat: object
        string that indicates which feature from 'data' must be used to group items
    res_feat: object
        string that indicates the measured feature (how many items correspond
    to the criterion)
    namesuffix: object
        string that ends the new feature name

    """
    md_ext = (data.groupby(grp_feat)[res_feat].agg({'min': "min",
                                              'med': "median",
                                              'max': "max"}).reset_index())
    colnames = ["n" + op + "_" + namesuffix for op in md_ext.columns.values[1:]]
    md_ext.columns = [grp_feat, *colnames]
    return pd.merge(metadata, md_ext, on=grp_feat, how='outer').fillna(0)

