# !/usr/bin/env python
# coding: utf-8

"""
Some utility functions designed for machine learning algorithm exploitation
"""

import pandas as pd
import numpy as np
import datetime


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

