# !/usr/bin/env python
# coding: utf-8

"""
Some utility functions designed for machine learning algorithm exploitation
"""

import re
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns

def compute_pca_variance(X):
    """Compute the covariance matrix of X and the associated eigen values to
    evaluate the explained variance of the data

    Parameters
    ----------
    X: numpy 2D array
        data matrix, contain the values of the dataframe used as a basis for
    the PCA
    
    """
    cov_mat = np.cov(X.T)
    eig_vals, eig_vecs = np.linalg.eig(cov_mat)
    eig_vals = sorted(eig_vals, reverse=True)
    tot = sum(eig_vals)
    varexp = [(i/tot)*100 for i in eig_vals]
    cumvarexp = np.cumsum(varexp)
    varmat = pd.DataFrame({'eig': eig_vals,
                           'varexp': varexp,
                           'cumvar': cumvarexp})[['eig','varexp','cumvar']]
    return varmat

def elbow_derivation(elbow, nbmin_clusters):
    """Compute a proxy of the elbow function derivative to automatically
    extract the optimal number of cluster; this number must be higher that nbmin_clusters

    Parameters
    ----------
    elbow: list
        contains value of the elbow function for each number of clusters
    nbmin_clusters: integer
        lower bound of the number of clusters
    
    """
    elbow_deriv = [0]
    for i in range(1, len(elbow)-1):
        if i < nbmin_clusters:
            elbow_deriv.append(0)
        else:
            elbow_deriv.append(elbow[i+1]+elbow[i-1]-2*elbow[i])
    return elbow_deriv

def one_feature_contribution(component_detail):
    """Describe and analyze the feature contribution to a component

    Parameters
    ----------
    component_detail: pd.Series
        contribution of each features (rows) to a PCA component (column)
    
    """
    tab = pd.DataFrame({'component': component_detail})
    tab['is_positive'] = tab.component > 0
    tab = abs(tab).sort_values(by='component', ascending=False).head(10)
    tab.loc[tab.is_positive==0, 'component'] = -tab.component
    most_important_features = pd.Series(tab.component)
    most_important_features.name = component_detail.name
    return most_important_features.sort_values(ascending=False)

def feature_contribution(pca_features):
    """Describe and analyze the feature contribution to each PCA component

    Parameters
    ----------
    pca_features: pd.DataFrame
        contribution of each features (rows) to each PCA components (columns)
    
    """
    best_contributions = []
    for col in pca_features:
        best_contributions.append(one_feature_contribution(pca_features[col]))
    return best_contributions

def plot_feature_contribution(feature_contributions, nb_subplot_col=2):
    """Plot the most important feature contributions for each PCA component;
    the chosen format is barplot, with 5 most positive and 5 most
    negative contributors (horizontal barplot with named labels)

    Parameters
    ----------
    feature_contributions: list of pd.DataFrames
        most important features for each PCA component
    
    """
    nb_components = len(feature_contributions)
    nb_vertical_plots = int(nb_components/nb_subplot_col)
    if nb_components%nb_subplot_col > 0:
        nb_vertical_plots = nb_vertical_plots + 1
    f, ax = plt.subplots(nb_vertical_plots, nb_subplot_col, figsize=(16, 12))
    for i in range(nb_components):
        data = feature_contributions[i].sort_values()
        ax_ = ax[int(i/nb_subplot_col)][i%nb_subplot_col]
        ax_.barh(np.arange(len(data)), data.values, tick_label=data.index)
        # ax_.set_xticklabels(data.index, rotation=30)
        ax_.axvline(0, color='k')
        ax_.set_xlim((-0.4,0.4))
        ax_.set_title(data.name)
    f.tight_layout()
    f.show()

    
def plot_feature_contribution_v2(feature_contributions, nb_subplot_col=2):
    """Plot the most important feature contributions for each PCA component;
    the chosen format is barplot, with 5 most positive and 5 most
    negative contributors (vertical coloured barplots)

    Parameters
    ----------
    feature_contributions: list of pd.DataFrames
        most important features for each PCA component
    
    """
    nb_components = len(feature_contributions)
    nb_vertical_plots = int(nb_components/nb_subplot_col)
    if nb_components%nb_subplot_col > 0:
        nb_vertical_plots = nb_vertical_plots + 1
    blue, red, green = sns.color_palette()[:3]
    f, ax = plt.subplots(nb_vertical_plots, nb_subplot_col, figsize=(16, 12))
    for i in range(nb_components):
        data = feature_contributions[i].sort_values()
        color_indices = split_md_features(data.index.values)
        bar_color = [sns.color_palette()[int(c)] for c in color_indices]
        edge_indices = color_indices * 10
        edge_indices[edge_indices > 9] = 0
        bar_bordercolor = [sns.color_palette()[int(c)] for c in edge_indices]
        bar_lw = np.repeat(0, len(data.index))
        bar_lw[color_indices%1!=0] = 5
        ax_ = ax[int(i/nb_subplot_col)][i%nb_subplot_col]
        ax_.bar(np.arange(len(data)), data.values,
                color=bar_color, edgecolor=bar_bordercolor, linewidth=bar_lw)
        ax_.axhline(0, color='k')
        ax_.set_ylim((-0.4,0.4))
        ax_.get_xaxis().set_visible(False)
        ax_.set_title(data.name)
        if i == 0:
            blue_patch = mpatches.Patch(color=blue,
                                        label='Contribution quantity feature')
            red_patch = mpatches.Patch(color=red, label='Version feature')
            green_patch = mpatches.Patch(color=green, label='Time feature')
            first_legend = ax_.legend(handles=[blue_patch,
                                               red_patch,
                                               green_patch])
        if i == 1:
            greyborder_patch = mpatches.Patch(edgecolor='grey', facecolor=blue,
                                              label='Node feature', lw=3)
            redborder_patch = mpatches.Patch(edgecolor=red, facecolor=blue,
                                             label='Way feature', lw=3)
            greenborder_patch = mpatches.Patch(edgecolor=green,
                                               facecolor=blue,
                                               label='Relation feature',
                                               lw=3)
            ax_.legend(handles=[greyborder_patch,
                                redborder_patch,
                                greenborder_patch])
    f.tight_layout()
    f.show()


def split_md_features(ft_names, element_type_splitting=True):
    """Split the metadata column into several types of features, e.g. quantity,
    version and time-related features, by returning a tuple of integer lists

    Parameters
    ----------
    ft_names: list
        List of feature names; key to split the features
    element_type_splitting: boolean
        split the quantity features according to the element type if True
    """
    quantity_indices = [re.search('^n[_m]', col) is not None for col in ft_names]
    version_indices = [re.search('^v[_m]', col) is not None for col in ft_names]
    time_indices = [re.search('^((activity)|([dt][_m]))', col) is not None
                    for col in ft_names]
    assert(sum(quantity_indices)+sum(version_indices)+sum(time_indices) ==
           len(ft_names) )
    synthesis = np.repeat(0.0, len(quantity_indices))
    synthesis[version_indices] = 1.0
    synthesis[time_indices] = 2.0
    node_indices = [re.search('_node', col) is not None for col in ft_names]
    way_indices = [re.search('_way', col) is not None for col in ft_names]
    rel_indices = [re.search('_relation', col) is not None for col in ft_names]
    synthesis[node_indices] = 0.1
    synthesis[way_indices] = 0.2
    synthesis[rel_indices] = 0.3
    return synthesis
