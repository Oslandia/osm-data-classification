# !/usr/bin/env python
# coding: utf-8

"""
Some utility functions aiming to analyse OSM data
"""

import pandas as pd
import numpy as np

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
def group_count(metadata, data, grp_feat, res_feat, namesuffix):
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
    md_ext = md_ext[[grp_feat, 'elem', 'node', 'way', 'relation']]
    colnames = "n_" + md_ext.columns.values[-4:] + namesuffix
    md_ext.columns = [grp_feat, *colnames]
    return pd.merge(metadata, md_ext, on=grp_feat, how='outer').fillna(0)

def group_nunique(metadata, data, grp_feat, res_feat, namesuffix):
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
    md_ext = md_ext[[grp_feat, 'elem', 'node', 'way', 'relation']]
    colnames = "n_" + md_ext.columns.values[-4:] + namesuffix
    md_ext.columns = [grp_feat, *colnames]
    return pd.merge(metadata, md_ext, on=grp_feat, how='outer').fillna(0)


def group_stats(metadata, data, grp_feat, res_feat, nameprefix, namesuffix):
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
    nameprefix: object
        string that begins the new feature name
    namesuffix: object
        string that ends the new feature name

    """
    md_ext = (data.groupby(grp_feat)[res_feat].agg({'min': "min",
                                                    'med': "median",
                                                    'max': "max"}).reset_index())
    # md_ext.med = md_ext.med.astype(int)
    md_ext = md_ext[[grp_feat, 'min', 'med', 'max']]
    colnames = [nameprefix + op + namesuffix for op in md_ext.columns.values[1:]]
    md_ext.columns = [grp_feat, *colnames]
    return pd.merge(metadata, md_ext, on=grp_feat, how='outer').fillna(0)

