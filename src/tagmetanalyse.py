# coding: utf-8

""" Implement some functions useful to analysis OSM tag genome """

import pandas as pd

########################################

def tagvalue_analysis(genome, key, pivot_var=['elem','version'], vrank=1):
    """Return a table that contains the number of unique elements for each tag
    value, element type and version, for a given tag key 

    INPUT: genome = pandas DataFrame with OSM element tag history, key = tag key
    that will be focused on, pivot_var = genome feature(s) taken into account
    to build the tag analysis, vrank = version number used to sort the
    resulting table

    """
    return (genome.query("tagkey==@key")
            .groupby(['tagvalue', *pivot_var])['id']
            .nunique()
            .unstack()
            .fillna(0))

def tagvalue_frequency(genome, key, pivot_var=['elem', 'version'], nround=2, vrank=1):
    """Return a table that contains the frequency of each tag value, for given
    element type, version and tag key 

    INPUT: genome = pandas DataFrame with OSM element tag history, key = tag
    key that will be focused on, pivot_var = genome feature(s) taken into
    account to build the tag analysis, nround = number of digits, vrank =
    version number used to sort the resulting table

    """
    total_uniqelem = (genome.query("tagkey==@key")
                      .groupby(pivot_var)['id']
                      .nunique()
                      .unstack()
                      .fillna(0))
    tagcount = tagvalue_analysis(genome, key, pivot_var=['elem','version'])
    tagcount_groups = tagcount.groupby(level='elem')
    tag_freq = []
    for key, group in tagcount_groups:
        tag_freq.append( group / total_uniqelem.loc[key])
    tag_freq = pd.concat(tag_freq)
    return (100*tag_freq).round(nround)

def tagkey_analysis(genome, pivot_var=['elem']):
    """Return a table that contains the number of unique elements for
    each tag key, element type and version 

    INPUT: genome = pandas DataFrame with OSM element tag history, pivot_var =
    genome feature(s) taken into account to build the tag analysis

    """
    return (genome.groupby(['tagkey', *pivot_var])['id']
            .nunique()
            .unstack()
            .fillna(0))

def tag_frequency(genome, pivot_var=['elem', 'version'], nround=2, vrank=1):
    """Return a table that contains the frequency of each tag key, for given
    element type and version

    INPUT: genome = pandas DataFrame with OSM element tag history,
    pivot_var = genome feature(s) taken into account to build
    the tag analysis, nround = number of digits, vrank = version number
    used to sort the resulting table

    """
    total_uniqelem = (genome
                      .groupby(pivot_var)['id']
                      .nunique()
                      .unstack()
                      .fillna(0))
    tagcount = tagkey_analysis(genome, pivot_var)
    tagcount_groups = tagcount.groupby(level='elem')
    tag_freq = []
    for key, group in tagcount_groups:
        tag_freq.append( group / total_uniqelem.loc[key])
    tag_freq = pd.concat(tag_freq)
    return 100*tag_freq.round(4)
