# !/usr/bin/env python
# coding: utf-8

"""
Some utility functions designed for machine learning algorithm exploitation
"""

import math
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

def plot_pca_variance(varmat):
    """Plot the PCA variance analysis: cumulated sum of explained variance as
    well as eigenvalues

    Parameters
    ----------
    varmat: pd.DataFrame
        PCA variance analysis results; contains three columns ('eig', 'varexp'
    and 'cumvar')
    
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

def plot_elbow(x, y):
    """Plot a range of kmeans inertia scores, so as to identify the best cluster
    quantity according to elbow method; overwriting of basic plot method

    Parameters
    ----------
    x: list
        units; typically from 1 to the max number of clusters
    y: list
        inertia scores
    
    """
    plt.plot(x, y)
    plt.show()
    
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

def plot_feature_contribution_v3(feature_contributions, ylim=0.5, nb_subplot_col=2):
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
        ax_.axvline(0, color='k')
        ax_.set_xlim((-ylim, ylim))
        ax_.set_title(data.name)
    f.tight_layout()
    f.show()


def plot_feature_contribution_v2(feature_contributions, ylim=0.5, nb_subplot_col=2):
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
    blue, red, green, purple = sns.color_palette()[:4]
    f, ax = plt.subplots(nb_vertical_plots, nb_subplot_col, figsize=(16, 12))
    for i in range(nb_components):
        data = feature_contributions[i].sort_values()
        color_indices = split_md_features(data.index.values)
        bar_color = [sns.color_palette()[int(c)] for c in color_indices]
        # edge_indices = color_indices * 10
        # edge_indices[edge_indices > 9] = 0
        # bar_bordercolor = [sns.color_palette()[int(c)] for c in edge_indices]
        # bar_lw = np.repeat(0, len(data.index))
        # bar_lw[color_indices%1!=0] = 5
        ax_ = ax[int(i/nb_subplot_col)][i%nb_subplot_col]
        ax_.bar(np.arange(len(data)), data.values, color=bar_color)
                # color=bar_color, edgecolor=bar_bordercolor, linewidth=bar_lw)
        ax_.axhline(0, color='k')
        ax_.set_ylim((-ylim, ylim))
        ax_.get_xaxis().set_visible(False)
        ax_.set_title(data.name)
        if i == 0:
            red_patch = mpatches.Patch(color=red, label='Node feature')
            green_patch = mpatches.Patch(color=green, label='Way feature')
            purple_patch = mpatches.Patch(color=purple, label='Relation feature')
            blue_patch = mpatches.Patch(color=blue,
                                        label='Other feature')
            first_legend = ax_.legend(handles=[red_patch,
                                               green_patch,
                                               purple_patch,
                                               blue_patch])
    f.tight_layout()
    f.show()

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
            data.Xclust.replace(to_replace={0:'Cluster_0',
                                            1:'Cluster_1',
                                            2:'Cluster_2'}, inplace=True)
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
