# !/usr/bin/env python
# coding: utf-8

"""
Some metadata plotting functions:
- 1D histograms
- 2D scatter plots
"""

import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

import utils

def md_scatter(metadata_x, metadata_y):
    """Draw a scatter plot from metadata features

    Parameters
    ----------
    metadata_x: pd.Series
        metadata that has to be plotted in abscissa
    metadata_x: pd.Series
        metadata that has to be plotted in ordinates
    
    """
    plt.plot(metadata_x, metadata_y, 'o')
    plt.xlabel(metadata_x.name)
    plt.ylabel(metadata_y.name)
    plt.tight_layout()
    plt.show()
   
def md_scatter_set(metadata, pattern, nb_subplot_col=2):
    """Draw 2D scatter plots from metadata features

    Parameters
    ----------
    metadata: pd.DataFrame
        metadata to plot
    pattern: object
        string designing the names of the features that have to be plotted
    nb_subplot_col: integer
        number of plots that must be draw horizontally
    
    """
    md_scatter = utils.extract_features(metadata, pattern)
    nb_components = len(md_scatter.columns)
    f, ax = plt.subplots(len(md_scatter.columns), len(md_scatter.columns),
                         figsize=(16, 12))
    for column in md_scatter:
        i = np.where(md_scatter.columns == column)[0][0]
        for column2 in md_scatter:
            if column != column2:
                j = np.where(md_scatter.columns == column2)[0][0]
                ax_ = ax[i][j]
                ax_.plot(md_scatter[column], md_scatter[column2], 'o')
                ax_.set_xlabel(column)
                ax_.set_ylabel(column2)
    f.tight_layout()
    f.show()

def md_hist(metadata_x, bins=np.linspace(0,1,21)):
    """Draw an histogram of the metadata_x feature

    Parameters
    ----------
    metadata_x: pd.Series
        metadata that has to be plotted in histogram
    bins: np.array
        array describing each histogram bin; the i-th bin is between the i-th
    and the (i+1)-th
    
    """
    plt.hist(metadata_x, bins=bins, normed=1)
    plt.xlabel("Histogram of "+metadata_x.name)
    plt.ylabel("Frequency (%)")
    plt.tight_layout()
    plt.show()

def md_hist_set(metadata, pattern, bins=np.linspace(0,1,51), nb_subplot_col=2):
    """Draw a set of histogram for each features of metadata that corresponds
    to the pattern

    Parameters
    ----------
    metadata: pd.DataFrame
        metadata that has to be plotted in histograms
    pattern: object
        string that designs the feature to plot (through regular expressions)
    bins: np.array
        array describing each histogram bin; the i-th bin is between the i-th
    and the (i+1)-th
    nb_subplot_col: integer
        number of plots that must be draw horizontally
    
    """
    md_hist = utils.extract_features(metadata, pattern)
    nb_components = len(md_hist.columns)
    nb_vertical_plots = int(nb_components/nb_subplot_col)
    if nb_components%nb_subplot_col > 0:
        nb_vertical_plots = nb_vertical_plots + 1
    f, ax = plt.subplots(nb_vertical_plots, nb_subplot_col, figsize=(16, 12))
    for column in md_hist:
        i = np.where(md_hist.columns == column)[0][0]
        data = md_hist[column]
        ax_ = ax[int(i/nb_subplot_col)][i%nb_subplot_col]
        ax_.hist(data, bins=bins, normed=1)
        ax_.set_ylim(0,50)
        ax_.set_title(data.name)
    f.tight_layout()
    f.show()

def md_multiplot(metadata, pattern):
    """Draw a generic plot of metadata features with 1D histogram and 2D
    scatter plots; use regex to identify features to plot

    Parameters
    ----------
    metadata: pd.DataFrame
        metadata that has to be plotted
    pattern: object
        string designing the feature names that has to be plotted
    
    """
    plotted_metadata = utils.extract_features(metadata, pattern)
    sns.pairplot(plotted_metadata)
    
def md_corplot(metadata, pattern):
    """ Draw a correlation plot of metadata features; consider only features
    that correspond to pattern
    """
    md_corplot = utils.extract_features(metadata, pattern)
    f, ax = plt.subplots(figsize=(12,10))
    sns.heatmap(md_corplot.astype(float).corr(),
                vmax=1,
                square=True,
                annot=True,
                annot_kws={'size':10},
                fmt='.2f')
    plt.xticks(rotation=90)
    plt.yticks(rotation=0)
    f.tight_layout()
    f.show()
