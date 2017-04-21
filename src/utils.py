# !/usr/bin/env python
# coding: utf-8

"""
Some utility functions aiming to analyse OSM data
"""

import pandas as pd
import numpy as np
from datetime import timedelta

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

def init_metadata(osm_elements, init_feat, duration_feat='activity_d',
                  timeunit='day'):
    """ This function produces an init metadata table based on 'init_feature'
    in table 'osm_elements'. The intialization consider timestamp measurements
    (generated for each metadata tables, i.e. elements, change sets and users).

    Parameters
    ----------
    osm_elements: pd.DataFrame
        OSM history data
    init_feat: object
        metadata basic feature name in string format
    duration_feat: object
        metadata duration feature name in string format
    timeunit: object
        time unit in which 'duration_feature' will be expressed

    Return
    ------
    metadata: pd.DataFrame
    metadata table with following features:
    init_feature (int) -- initializing feature ID
    first_at (datetime) -- first timestamp
    last_at (datetime) -- last timestamp
    activity (int) -- activity (in 'timeunit' format)
    
    """
    metadata = (osm_elements.groupby(init_feat)['ts']
                .agg({'first_at':"min", 'last_at':"max"})
                .reset_index())
    metadata[duration_feat] = metadata.last_at - metadata.first_at
    if timeunit == 'second':
        metadata[duration_feat] = (metadata[duration_feat] /
                                   timedelta(seconds=1))
    if timeunit == 'minute':
        metadata[duration_feat] = (metadata[duration_feat] /
                                   timedelta(minutes=1))
    if timeunit == 'hour':
        metadata[duration_feat] = metadata[duration_feat] / timedelta(hours=1)
    if timeunit == 'day':
        metadata[duration_feat] = metadata[duration_feat] / timedelta(days=1)
    return metadata


def extract_elem_metadata(osm_elements):
    """ Extract element metadata from OSM history data

    Parameters
    ----------
    osm_elements: pd.DataFrame
        OSM history data
    
    Return
    ------
    elem_md: pd.DataFrame
        Change set metadata with timestamp information, user-related features
    and other features describing modification and OSM elements themselves
    
    """
    elem_md = init_metadata(osm_elements, ['elem','id'], 'lifecycle_d')
    elem_md['version'] = (osm_elements.groupby(['elem','id'])['version']
                       .max()
                       .reset_index())['version']
    elem_md = pd.merge(elem_md, osm_elements[['elem','id','version','visible']],
                       on=['elem', 'id', 'version'])
    elem_md['n_chgset'] = (osm_elements.groupby(['elem', 'id'])['chgset']
                           .nunique()
                           .reset_index())['chgset']
    elem_md['n_user'] = (osm_elements.groupby(['elem', 'id'])['uid']
                         .nunique()
                         .reset_index())['uid']
    elem_md['n_autocorr'] = (osm_elements
                             .groupby(['elem','id'])['willbe_autocorr']
                             .sum()
                             .reset_index()['willbe_autocorr']
                             .astype('int'))
    elem_md['n_corr'] = (osm_elements
                             .groupby(['elem','id'])['willbe_corr']
                             .sum()
                             .reset_index()['willbe_corr']
                             .astype('int'))
    return elem_md

def extract_chgset_metadata(osm_elements):
    """ Extract change set metadata from OSM history data

    Parameters
    ----------
    osm_elements: pd.DataFrame
        OSM history data
    
    Return
    ------
    chgset_md: pd.DataFrame
        Change set metadata with timestamp information, user-related features
    and other features describing modification and OSM elements themselves
    
    """
    chgset_md = init_metadata(osm_elements, 'chgset', 'duration_m', 'minute')
    chgset_md = pd.merge(chgset_md, osm_elements[['elem','id','uid']],
                         on=['elem','id'])
    
    return chgset_md

def extract_user_metadata(osm_elements):
    """ Extract user metadata from OSM history data

    Parameters
    ----------
    osm_elements: pd.DataFrame
        OSM history data
    
    Return
    ------
    user_md: pd.DataFrame
        User metadata with timestamp information, user-related features
    and other features describing modification and OSM elements themselves
    
    """
    user_md = init_metadata(osm_elements, 'uid')
    return user_md
