# !/usr/bin/env python
# coding: utf-8

"""
Some utility functions designed for machine learning algorithm exploitation
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

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
    contribution_rank = component_detail.sort_values(ascending=False)
    most_important_features = pd.concat([contribution_rank.head(5),
                                         contribution_rank.tail(5)])
    return most_important_features

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
    f, ax = plt.subplots(nb_vertical_plots, nb_subplot_col, figsize=(16, 12))
    for i in range(nb_components):
        data = feature_contributions[i].sort_values()
        ax_ = ax[int(i/nb_subplot_col)][i%nb_subplot_col]
        ax_.bar(np.arange(len(data)), data.values)
        ax_.axhline(0, color='k')
        ax_.set_ylim((-0.4,0.4))
        ax_.get_xaxis().set_visible(False)
        ax_.set_title(data.name)
    f.tight_layout()
    f.show()
