# coding: utf-8

"""
Some utility functions designed for machine learning algorithm exploitation
"""

import math
import re
import random

import pandas as pd
import numpy as np

from sklearn.metrics import silhouette_score

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches


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
    eig_margin = np.diff(eig_vals)
    eig_margin = list(-1*np.insert(eig_margin, len(eig_margin), np.NaN))
    tot = sum(eig_vals)
    varexp = [(i/tot)*100 for i in eig_vals]
    var_margin = np.diff(varexp)
    var_margin = list(-1*np.insert(var_margin, len(var_margin), np.NaN))
    cumvarexp = np.cumsum(varexp)
    varmat = pd.DataFrame({'eig': eig_vals,
                           'margineig': eig_margin,
                           'varexp': varexp,
                           'cumvar': cumvarexp,
                           'marginvar': var_margin})[['eig','margineig',
                                                       'varexp','cumvar',
                                                       'marginvar']]
    return varmat

def optimal_PCA_components(variance, nb_min_dim, nb_max_dim, standard_norm):
    """Return a number of components that is supposed to be optimal,
    regarding the variance matrix (sufficient explained variance threshold, low
    eigenvalues when data was normalized in a standard way)

    varaince: nd.array
    nb_dim_min: int
    nb_dim_max: int
    standard_norm: bool

    Return the "optimal" number or PCA components
    """
    candidate_npc = 0
    if standard_norm:
        for i in range(len(variance)):
            if variance.iloc[i, 0] < 1 or variance.iloc[i, 2] > 70:
                candidate_npc = i + 1
                break
    else:
        for i in range(len(variance)):
            if variance.iloc[i, 2] > 70:
                candidate_npc = i + 1
                break
    if candidate_npc < nb_min_dim:
        candidate_npc = nb_min_dim
    if candidate_npc > nb_max_dim:
        candidate_npc = nb_max_dim
    return candidate_npc

def plot_pca_variance(variance_matrix, nb_max_dimension):
    """Plot the PCA variance analysis: cumulated sum of explained variance as
    well as eigenvalues

    Parameters
    ----------
    varmat: pd.DataFrame
        PCA variance analysis results; contains three columns ('eig', 'varexp'
    and 'cumvar')
    nb_max_dimension: integer
        Maximal number of plotted dimensions
    
    """
    f, ax = plt.subplots(2,1)
    ax[0].bar(range(1,1+len(varmat)), varmat['varexp'].values, alpha=0.25, 
            align='center', label='individual explained variance', color = 'g')
    ax[0].step(range(1,1+len(varmat)), varmat['cumvar'].values, where='mid',
             label='cumulative explained variance')
    ax[0].axhline(70, color="blue", linestyle="dotted")
    ax[0].legend(loc='best')
    ax[1].bar(range(1,1+len(varmat)), varmat['eig'].values, alpha=0.25,
              align='center', label='eigenvalues', color='r')
    ax[1].axhline(1, color="red", linestyle="dotted")
    ax[1].legend(loc="best")
    f.show()
    return f

def plot_cluster_decision(x, y1, y2):
    """Plot a range of kmeans inertia scores and silhouette boxplots, so as to
    identify the best cluster quantity according to elbow method; overwriting
    of basic plot method

    Parameters
    ----------
    x: list
        units; typically from 1 to the max number of clusters
    y1: list
        inertia scores
    y2: list of lists
        silhouette samples
    """
    f, ax = plt.subplots(2, 1)
    ax[0].plot(x, y1)
    ax[0].set_ylabel("inertia")
    ax[1].boxplot(y2, labels=x)
    ax[1].set_xlabel('clusters number')
    ax[1].set_ylabel("silhouette")
    ax[0].set_title("Elbow and silhouette for KMeans")
    plt.tight_layout()
    return f

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
    """Describe and analyze the feature contribution to a component; select the
    10 most important values (in absolute value) among component_detail and
    sort them in descending order

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

def plot_feature_contribution(data):
    """Plot feature contribution by using seaborn heatmap capability

    Parameters
    ----------
    data: pd.DataFrame
        data to plot: contributions to PCA components
    
    """
    f, ax = plt.subplots(figsize=(10,12))
    sns.heatmap(data, annot=True, fmt='.3f', ax=ax)
    plt.yticks(rotation=0)
    plt.tight_layout()
    plt.show()
    
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
    node_indices = [re.search('_node', col) is not None for col in ft_names]
    way_indices = [re.search('_way', col) is not None for col in ft_names]
    relation_indices = [re.search('_relation', col) is not None
                    for col in ft_names]
    synthesis = np.repeat(0, len(ft_names))
    synthesis[node_indices] = 1
    synthesis[way_indices] = 2
    synthesis[relation_indices] = 3
    return synthesis

SUBPLOT_LAYERS = pd.DataFrame({'x':[0,0,0,1,1,2],
                               'y':[1,2,3,2,3,3],
                               'nb_comp':[2,3,4,3,4,4]})

def correlation_circle(pcavar, pcaind=None, pattern='', nb_comp=2, threshold=0.1, explained=None):
    """ Draw a correlation circle form PCA results

    Parameters
    ----------
    pcavar: pandas.DataFrame
       feature contributions to each component (n_features, n_components)
    scores: pandas.DataFrame
       contribution of each individuals to each component (n_observation,
    n_features)
    pattern: str
       character string that indicates which feature has to be kept
    nb_comp: integer
       number of components that have to be plotted in the circle
    threshold: float
       indicates a lower bound of feature contribution norms, to avoid
    unreadable arrow draws; between 0 and 1
    explained: list
       vector of variance proportion explained by each PCA component
    """
    if nb_comp < 2:
        raise ValueError("Invalid number of PCA components, choose a number between 2 and 4!")
    if nb_comp > 4:
        raise ValueError("Two many components: can't plot them properly!")
    loadings = pcavar.loc[pcavar.index.str.contains(pattern)]
    nb_plots = int(nb_comp*(nb_comp-1)/2)
    if nb_comp == 2:
        nb_vertical_plots = 1
        nb_horiz_plots = 1
    elif nb_comp == 3:
        nb_vertical_plots = 1
        nb_horiz_plots = 3
    elif nb_comp == 4:
        nb_vertical_plots = 2
        nb_horiz_plots = 3
    f, ax = plt.subplots(nb_vertical_plots, nb_horiz_plots, figsize=(6*nb_horiz_plots, 6*nb_vertical_plots))
    subplot_layers = SUBPLOT_LAYERS.query('nb_comp <= @nb_comp')
    for i in range(nb_plots):
        if nb_comp == 2:
            ax_ = ax
        elif nb_comp == 3:
            ax_ = ax[i%nb_horiz_plots]
        elif nb_comp == 4:
            ax_ = ax[int(i/nb_horiz_plots)][i%nb_horiz_plots]
        x_comp, y_comp = subplot_layers.iloc[i][['x', 'y']]
        # Circle structure
        circle = plt.Circle((0, 0), radius=1, color='grey', fill=False)
        ax_.add_artist(circle)
        # Plot rescaled individuals contributions on circle
        if pcaind is not None:
            scalex = 1.0 / np.ptp(pcaind.iloc[:, x_comp])
            scaley = 1.0 / np.ptp(pcaind.iloc[:, y_comp])
            score_x = pcaind.copy().iloc[:, x_comp] * scalex
            score_y = pcaind.copy().iloc[:, y_comp] * scaley
            ax_.scatter(score_x, score_y, marker='.')
        # Draw arrows to materialize feature contributions
        for name, feature in loadings.iterrows():
            x, y = feature.iloc[x_comp], feature.iloc[y_comp]
            if math.sqrt(x ** 2 + y ** 2) < threshold:
                continue
            arrows = ax_.arrow(0, 0, x, y, width=0.01, color='r', alpha=0.5)
            ax_.annotate(name, xy=(x,y), xytext=(x+0.02,y+0.02), color='r', alpha=1)
        # Graphical statements: make the circle sexier
        xl = 'PC' + str(x_comp+1)
        yl = 'PC' + str(y_comp+1)
        xl = xl if explained is None else xl + ' ({:.2f}%)'.format(100. * explained[x_comp])
        yl = yl if explained is None else yl + ' ({:.2f}%)'.format(100. * explained[y_comp])
        ax_.set_xlabel(xl)
        ax_.set_ylabel(yl)
        ax_.set_xlim((-1.1, 1.1))
        ax_.set_ylim((-1.1, 1.1))
    plt.legend(["Individuals"], loc=0)
    plt.tight_layout()
    plt.show()

def contrib_barplot(data, best=10):
    """Highlight best PCA contributors (either features or individuals), by
    considering the contribution sum of squares, in the chosen PCA

    Parameters
    ----------
    data: pandas.DataFrame
        data to plot
    best: integer
        number of contributors (features or individuals) to highlight, default
    to 10
    
    """
    contribs = data.apply(lambda x: sum(x**2), axis=1).sort_values().tail(best)
    plt.barh(np.arange(best), contribs.values, tick_label=contribs.index)
    plt.tight_layout()
    plt.show()

def plot_individual_contribution(data, nb_comp=2, explained=None, best=None,
                                 cluster=None, cluster_centers=None):
    """Plot individual contributions to PCA components
    
    Parameters
    ----------
    data: pandas.DataFrame
        data to plot
    best: integer
        number of contributors (features or individuals) to highlight, default
    to 10
    comp: list of two integers
        components onto which individuals have to be plotted
    
    """
    if nb_comp < 2:
        raise ValueError("Invalid number of PCA components (choose 2, 3 or 4)!")
    if nb_comp > 4:
        raise ValueError("Two many components: can't plot them properly!")
    nb_plots = int(nb_comp*(nb_comp-1)/2)
    if nb_comp == 2:
        nb_vertical_plots = 1
        nb_horiz_plots = 1
    elif nb_comp == 3:
        nb_vertical_plots = 1
        nb_horiz_plots = 3
    elif nb_comp == 4:
        nb_vertical_plots = 2
        nb_horiz_plots = 3
    f, ax = plt.subplots(nb_vertical_plots, nb_horiz_plots,
                         figsize=(6*nb_horiz_plots, 6*nb_vertical_plots))
    subplot_layers = SUBPLOT_LAYERS.query('nb_comp <= @nb_comp')
    for i in range(nb_plots):
        if nb_comp == 2:
            ax_ = ax
        elif nb_comp == 3:
            ax_ = ax[i%nb_horiz_plots]
        elif nb_comp == 4:
            ax_ = ax[int(i/nb_horiz_plots)][i%nb_horiz_plots]
        comp = subplot_layers.iloc[i][['x', 'y']]
        x_column = 'PC'+str(1+comp[0])
        y_column = 'PC'+str(1+comp[1])
        if cluster is not None:
            for name, group in data.groupby('Xclust'):
                ax_.plot(group[x_column], group[y_column], marker='.',
                         linestyle='', ms=10, label=name)
                if i == 0:
                    ax_.legend(loc=0)
            if cluster_centers is not None:
                ax_.plot(cluster_centers[[x_column]],
                         cluster_centers[[y_column]],
                         'kD', markersize=10)
                for i, point in cluster_centers.iterrows():
                    ax_.text(point[x_column]-0.2, point[y_column]-0.2,
                             ('C'+str(i)+' (n='
                              +str(int(point['n_individuals']))+')'),
                             weight='bold', fontsize=14)
        else:
            ax_.plot(data.iloc[:,comp[0]],
                     data.iloc[:,comp[1]],
                     '.', markersize=10)
            if best is not None:
                contribs = (data.apply(lambda x: sum(x**2), axis=1)
                            .sort_values()
                            .tail(best))
                best_ind = data.loc[contribs.index]
                ax_.plot(best_ind.iloc[:,comp[0]],
                         best_ind.iloc[:,comp[1]],
                         'r*', markersize=10)
        x_column = (x_column if explained is None
              else x_column + ' ({:.2f}%)'.format(explained[comp[0]]))
        y_column = (y_column if explained is None
              else y_column + ' ({:.2f}%)'.format(explained[comp[1]]))
        ax_.set_xlabel(x_column)
        ax_.set_ylabel(y_column)
    plt.tight_layout()
    plt.show()


def compute_nb_clusters(features, centers, labels, nbmin_clusters=3):
    """Try to find the optimal number of KMeans clusters.

    Compute KMeans inertia for each cluster number until nbmax_clusters+1 to
    find the optimal number of clusters. Use the elbow method

    features: list of nd.array
        shape (Nrows, Ncols)
    centers: list of nd.array
        shape (Nclusters, Ncols)
    labels: list of nd.array
        shape (Nrows, )

    Return the "optimal" number of clusters
    """
    scores = []
    for feature, center, label in zip(features, centers, labels):
        # compute the inertia
        inertia = np.sum((feature - center[label]) ** 2, dtype=np.float64)
        scores.append(inertia)
    elbow_deriv = elbow_derivation(scores, nbmin_clusters)
    return 1 + elbow_deriv.index(max(elbow_deriv))


def kmeans_elbow_silhouette(features, centers, labels,
                            nbmin_clusters, nbmax_clusters):
    """Compute the KMeans elbow and silhouette scores and plot them

    features: list of nd.array
        shape (Nrows, Ncols)
    centers: list of nd.array
        shape (Nclusters, Ncols)
    labels: list of nd.array
        shape (Nrows, )
    nbmin_clusters: int
    nbmax_clusters: int

    Return a figure. Two subplots: Elbow and Silhouette
    """
    # scores for elbow
    scores = []
    silhouette = []
    for feature, center, label in zip(features, centers, labels):
        inertia = np.sum((feature - center[label]) ** 2, dtype=np.float64)
        scores.append(inertia)
        silhouette_avg = []
        for k in range(10):
            s = random.sample(range(len(feature)), 2000)
            Xsampled = feature[s]
            Csampled = label[s]
            while(len(np.unique(Csampled)) == 1):
                s = random.sample(range(len(feature)), 2000)
                Xsampled = feature[s]
                Csampled = label[s]
            silhouette_avg.append(silhouette_score(X=Xsampled,
                                                   labels=Csampled))
        silhouette.append(silhouette_avg)
    return plot_cluster_decision(range(nbmin_clusters, nbmax_clusters + 1),
                                 scores, silhouette)
