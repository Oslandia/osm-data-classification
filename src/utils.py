# !/usr/bin/env python
# coding: utf-8

"""
Some utility functions aiming to analyse OSM data
"""

import datetime as dt
import pandas as pd
import numpy as np
from datetime import timedelta
import re
import math
import statsmodels.api as sm

from extract_user_editor import editor_name

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

def osm_stats(osm_history, timestamp):
    """Compute some simple statistics about OSM elements (number of nodes,
    ways, relations, number of active contributors, number of change sets

    Parameters
    ----------
    osm_history: df
        OSM element up-to-date at timestamp
    timestamp: datetime
        date at which OSM elements are evaluated
    """
    osmdata = datedelems(osm_history, timestamp)
#    nb_nodes, nb_ways, nb_relations = list(osm_data.elem.value_counts())
    nb_nodes = len(osmdata.query('elem=="node"'))
    nb_ways = len(osmdata.query('elem=="way"'))
    nb_relations = len(osmdata.query('elem=="relation"'))
    nb_users = osmdata.uid.nunique()
    nb_chgsets = osmdata.chgset.nunique()
    return [nb_nodes, nb_ways, nb_relations, nb_users, nb_chgsets]

def osm_chronology(history, start_date, end_date=dt.datetime.now()):
    """Evaluate the chronological evolution of OSM element numbers

    Parameters
    ----------
    history: df
        OSM element timeline

    """
    timerange = pd.date_range(start_date, end_date, freq="1M").values
    osmstats = [osm_stats(history, str(date)) for date in timerange]
    osmstats = pd.DataFrame(osmstats, index=timerange,
                            columns=['n_nodes', 'n_ways', 'n_relations',
                                     'n_users', 'n_chgsets'])
    return osmstats

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
    statistic features (first and ninth deciles) corresponding to each
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
    md_ext = (data.groupby(grp_feat)[res_feat]
              .quantile(q=[0.1,0.9])
              .unstack()
              .reset_index())
    colnames = [nameprefix + str(int(100*op)) + namesuffix
                 for op in md_ext.columns.values[1:]]
    md_ext.columns = [grp_feat, *colnames]
    return pd.merge(metadata, md_ext, on=grp_feat, how='outer').fillna(0)

def init_metadata(osm_elements, init_feat, timeunit='1d'):
    """ This function produces an init metadata table based on 'init_feature'
    in table 'osm_elements'. The intialization consider timestamp measurements
    (generated for each metadata tables, i.e. elements, change sets and users).

    Parameters
    ----------
    osm_elements: pd.DataFrame
        OSM history data
    duration_feat: object
        metadata duration feature name in string format
    timeunit: object
        time unit in which 'duration_feature' will be expressed

    """
    metadata = (osm_elements.groupby(init_feat)['ts']
                .agg(["min", "max"])
                .reset_index())
    metadata.columns = [*init_feat, 'first_at', 'last_at']
    metadata['lifespan'] = ((metadata.last_at - metadata.first_at)
                            / pd.Timedelta(timeunit))
    extraction_date = osm_elements.ts.max()
    metadata['n_inscription_days'] = ((extraction_date - metadata.first_at)
                                      / pd.Timedelta('1d'))
    metadata['n_activity_days'] = (osm_elements
                                   .groupby(init_feat)['ts']
                                   .nunique()
                                   .reset_index())['ts']
    # nb_inscription_day
    return metadata.sort_values(by=['first_at'])

def enrich_osm_elements(osm_elements):
    """Enrich OSM history data by computing additional features

    Parameters
    ----------
    osm_elements: pd.DataFrame
        OSM history data

    """
    # Extract information from first and last versions
    osmelem_first_version = (osm_elements
                             .groupby(['elem','id'])['version', 'uid']
                             .first()
                             .reset_index())
    osm_elements = pd.merge(osm_elements, osmelem_first_version,
                                on=['elem','id'])
    osm_elements.columns = ['elem', 'id', 'version', 'visible', 'ts',
                                'uid', 'chgset', 'vmin', 'first_uid']
    osmelem_last_version = (osm_elements
                             .groupby(['elem','id'])['version', 'uid',
                                                     'visible']
                             .last()
                             .reset_index())
    osm_elements = pd.merge(osm_elements, osmelem_last_version,
                                on=['elem','id'])
    osm_elements.columns = ['elem', 'id', 'version', 'visible', 'ts', 'uid',
                            'chgset', 'vmin', 'first_uid', 'vmax', 'last_uid',
                            'available']
    osmelem_last_bychgset = (osm_elements
                             .groupby(['elem','id','chgset'])['version',
                                                              'visible']
                             .last()
                             .reset_index())
    osm_elements = pd.merge(osm_elements,
                            osmelem_last_bychgset[['elem', 'id',
                                                   'chgset', 'visible']],
                            on=['elem','id', 'chgset'])
    osm_elements.columns = ['elem', 'id', 'version', 'visible', 'ts', 'uid',
                            'chgset', 'vmin', 'first_uid', 'vmax', 'last_uid',
                            'available', 'open']

    # New version-related features
    osm_elements['init'] = osm_elements.version == osm_elements.vmin
    osm_elements['up_to_date'] = osm_elements.version == osm_elements.vmax
    osm_elements = osm_elements.drop(['vmin'], axis=1)

    osmelem_first_bychgset = (osm_elements
                             .groupby(['elem','id','chgset'])['version', 'init']
                             .first()
                             .reset_index())
    osm_elements = pd.merge(osm_elements,
                            osmelem_first_bychgset[['elem', 'id',
                                                    'chgset', 'init']],
                            on=['elem','id','chgset'])
    osm_elements.columns = ['elem', 'id', 'version', 'visible', 'ts', 'uid',
                            'chgset', 'first_uid', 'vmax', 'last_uid',
                            'available', 'open', 'init', 'up_to_date',
                            'created']

    # Whether or not an element will be corrected in the last version
    osm_elements['willbe_corr'] = np.logical_and(osm_elements.id.diff(-1)==0,
                                              osm_elements.uid.diff(-1)!=0)
    osm_elements['willbe_autocorr'] = np.logical_and(osm_elements.id.diff(-1)==0,
                                                     osm_elements.uid
                                                     .diff(-1)==0)

    # Time before the next modification
    osm_elements['nextmodif_in'] = - osm_elements.ts.diff(-1)
    osm_elements.loc[osm_elements.up_to_date,['nextmodif_in']] = pd.NaT
    osm_elements.nextmodif_in = (osm_elements.nextmodif_in
                                 .astype('timedelta64[D]'))

    # Time before the next modification, if it is done by another user
    osm_elements['nextcorr_in'] = osm_elements.nextmodif_in
    osm_elements['nextcorr_in'] = (osm_elements.nextcorr_in
                                   .where(osm_elements.willbe_corr,
                                          other=pd.NaT))

    # Time before the next modification, if it is done by the same user
    osm_elements['nextauto_in'] = osm_elements.nextmodif_in
    osm_elements['nextauto_in'] = (osm_elements.nextauto_in
                                   .where(osm_elements.willbe_autocorr,
                                                   other=pd.NaT))

    return osm_elements

def extract_elem_metadata(osm_elements, drop_ts=True):
    """ Extract element metadata from OSM history data

    Parameters
    ----------
    osm_elements: pd.DataFrame
        OSM history data

    Return
    ------
    elem_md: pd.DataFrame
        Change set metadata with timestamp information, version-related features
    and number of unique change sets (resp. users)

    """
    elem_md = init_metadata(osm_elements, ['elem','id'])
    elem_md['version'] = (osm_elements.groupby(['elem','id'])['version']
                          .max()
                          .reset_index())['version']
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
    elem_md = pd.merge(elem_md, osm_elements[['elem', 'id',
                                              'version', 'visible']],
                       on=['elem', 'id', 'version'])
    elem_md = elem_md.set_index(['elem', 'id'])
    if drop_ts:
        return drop_features(elem_md, '_at')
    else:
        return elem_md

def extract_chgset_metadata(osm_elements, drop_ts=True):
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
    chgset_md = init_metadata(osm_elements, ['chgset'], '1m')
    # User-related features
    chgset_md = pd.merge(chgset_md,
                         osm_elements[['chgset','uid']].drop_duplicates(),
                         on=['chgset'])
    chgset_md['user_lastchgset_h'] = (chgset_md.groupby('uid')['first_at']
                                      .diff())
    chgset_md.user_lastchgset_h = (chgset_md.user_lastchgset_h /
                                   timedelta(hours=1))
    chgset_md['user_chgset_rank'] = chgset_md.groupby('uid')['first_at'].rank()
    # Update features
    chgset_md = group_stats(chgset_md, osm_elements, 'chgset', 'nextmodif_in',
                              't', '_update_d')
    # Number of modifications per unique element
    contrib_byelem = (osm_elements.groupby(['elem', 'id', 'chgset'])['version']
                      .count()
                      .reset_index())
    chgset_md = group_stats(chgset_md, contrib_byelem, 'chgset', 'version',
                              'n', '_modif_byelem')
    # Element-related features
    chgset_md = extract_element_features(chgset_md, osm_elements,
                                       'node', 'chgset')
    chgset_md = extract_element_features(chgset_md, osm_elements,
                                       'way', 'chgset')
    chgset_md = extract_element_features(chgset_md, osm_elements,
                                       'relation', 'chgset')
    chset_md = chgset_md.set_index('chgset')
    if drop_ts:
        return drop_features(chgset_md, '_at')
    else:
        return chgset_md

def metadata_version(metadata, osmelem, grp_feat, res_feat, feature_suffix):
    """Compute the version-related features of metadata and append them into
    the metadata table

    Parameters
    ----------
    metadata: pd.DataFrame
        Metadata table to complete
    osmelem: pd.DataFrame
        original data used to compute versions; contains a 'elem' feature
    grp_feat: object
        string that indicates which feature from 'data' must be used to group items
    res_feat: object
        string that indicates the measured feature (how many items correspond
    feature_suffix: str
        string designing the end of the new feature names
    """
    osmelem_nodes = osmelem.query('elem=="node"')
    osmelem_ways = osmelem.query('elem=="way"')
    osmelem_relations = osmelem.query('elem=="relation"')
    metadata = group_stats(metadata, osmelem_nodes, grp_feat, res_feat,
                              'v', '_node'+feature_suffix)
    metadata = group_stats(metadata, osmelem_ways, grp_feat, res_feat,
                              'v', '_way'+feature_suffix)
    metadata = group_stats(metadata, osmelem_relations, grp_feat, res_feat,
                              'v', '_relation'+feature_suffix)
    return metadata

def extract_user_metadata(osm_elements, chgset_md, drop_ts=True):
    """ Extract user metadata from OSM history data

    Parameters
    ----------
    osm_elements: pd.DataFrame
        OSM history data
    chgset_md: pd.DataFrame
        OSM change set metadata

    Return
    ------
    user_md: pd.DataFrame
        User metadata with timestamp information, changeset-related features
    and other features describing modification and OSM elements themselves

    """
    user_md = init_metadata(osm_elements, ['uid'])
    # Change set-related features
    user_md['n_chgset'] = (chgset_md.groupby('uid')['chgset']
                           .count()
                           .reset_index())['chgset']
    user_md['dmean_chgset'] = (chgset_md.groupby('uid')['lifespan']
                               .mean()
                               .reset_index())['lifespan']
    # Number of modifications per unique element
    contrib_byelem = (osm_elements.groupby(['elem', 'id', 'uid'])['version']
                      .count()
                      .reset_index())
    user_md['nmean_modif_byelem'] = (contrib_byelem.groupby('uid')['version']
                                    .mean()
                                     .reset_index())['version']
    # Modification-related features
    user_md = extract_modif_features(user_md, osm_elements, 'node', 'uid')
    user_md = extract_modif_features(user_md, osm_elements, 'way', 'uid')
    user_md = extract_modif_features(user_md, osm_elements, 'relation', 'uid')
    user_md = user_md.set_index('uid')
    if drop_ts:
        return drop_features(user_md, '_at')
    else:
        return user_md

def add_chgset_metadata(metadata, total_change_sets):
    """Add total change set count to user metadata

    Parameters
    ----------
    metadata: pd.DataFrame
        user metadata; must be indexed by a column 'uid'
    total_change_sets: pd.DataFrame
        total number of change sets by user; must contain columns 'uid' and 'num'

    """
    metadata = (metadata.join(total_change_sets.set_index('uid'))
                .rename_axis({'num': 'n_total_chgset'}, axis=1))
    metadata['p_local_chgset'] = metadata.n_chgset / metadata.n_total_chgset
    return metadata

def add_editor_metadata(metadata, top_editors):
    """Add editor information to each metadata recordings; use an outer join to
    overcome the fact that some users do not indicate their editor, and may be
    skipped by a natural join => the outer join allow to keep them with 0
    values on known editors

    Parameters
    ----------
    metadata: pd.DataFrame
        user metadata; must be indexed by a column 'uid'
    top_editors: pd.DataFrame
        raw editor information, editors used by each user, with a highlight on
    N most popular editors; must contain a column 'uid'

    """
    metadata = (metadata
                .join(top_editors.set_index('uid'), how='left')
                .fillna(0))
    metadata['n_total_chgset_unknown'] = (metadata['n_total_chgset']
                                          - metadata['n_total_chgset_known'])
    return drop_features(metadata, 'n_total_chgset_known')

def transform_editor_features(metadata):
    """Transform editor-related features into metadata; editor uses are expressed
    as proportions of n_total_chgset, a proportion of local change sets is
    computed as a new feature and an ecdf transformation is applied on n_chgset
    and n_total_chgset
    
    Parameters
    ----------
    metadata: pd.DataFrame
        user metadata; must contain n_chgset and n_total_chgset columns, and
    editor column names must begin with 'n_total_chgset_'

    """
    normalize_features(metadata, 'n_total_chgset')
    metadata = ecdf_transform(metadata, 'n_chgset')
    metadata = ecdf_transform(metadata, 'n_total_chgset')
    return metadata

def ecdf_transform(metadata, feature):
    """ Apply an ECDF transform on feature within metadata; transform the column
    data into ECDF values

    Parameters
    ----------
    metadata: pd.DataFrame
        Metadata in which the transformation takes place
    feature: object
        string designing the transformed column

    Return
    ------
    New metadata, with a new renamed feature containing ecdf version of
    original data

    """
    ecdf = sm.distributions.ECDF(metadata[feature])
    metadata[feature] = ecdf(metadata[feature])
    new_feature_name = 'u_' + feature.split('_', 1)[1]
    return metadata.rename(columns={feature: new_feature_name})

def extract_modif_features(metadata, data, element_type, grp_feat):
    """Extract a set of metadata features corresponding to a specific element
    type; centered on modifications

    Parameters
    ----------
    metadata: pd.DataFrame
        Metadata table
    data: pd.DataFrame
        Original data
    element_type: object
        string designing the element type ("node", "way", or "relation")
    grp_feat: object
        string designing the grouping feature; it characterizes the metadata
    ("chgset", or "user")

    """
    typed_data = data.query('elem==@element_type')
    metadata = create_count_features(metadata, element_type, typed_data,
                               grp_feat, 'id', '')
    metadata = create_count_features(metadata, element_type,
                               typed_data.query("init"),
                               grp_feat, 'id', "_cr")
    metadata = create_count_features(metadata, element_type,
                               typed_data.query("not init and visible"),
                               grp_feat, 'id', "_imp")
    metadata = create_count_features(metadata, element_type,
                               typed_data.query("not init and not visible"),
                               grp_feat, 'id', "_del")
    metadata = create_count_features(metadata, element_type,
                               typed_data.query("up_to_date"),
                               grp_feat, 'id', "_utd")
    metadata = create_count_features(metadata, element_type,
                               typed_data.query("willbe_corr"),
                               grp_feat, 'id', "_cor")
    metadata = create_count_features(metadata, element_type,
                               typed_data.query("willbe_autocorr"),
                               grp_feat, 'id', "_autocor")
    # normalize_features(metadata, 'n_'+element_type+'_modif')
    return metadata

def create_count_features(metadata, element_type, data, grp_feat, res_feat, feature_suffix):
    """Create an additional feature to metadata by counting number of
    occurrences in data, for a specific element_type

    Parameters
    ----------
    metadata: pd.DataFrame
        Metadata table
    element_type: object
        string designing the element type ("node", "way", or "relation")
    data: pd.DataFrame
        Original data
    grp_feat: object
        string designing the grouping feature; it characterizes the metadata
    ("chgset", or "user")
    res_feat: object
        string that indicates the measured feature (how many items correspond
    feature_suffix: type
        description

    """
    feature_name = 'n_'+ element_type + '_modif' + feature_suffix
    newfeature = (data.groupby([grp_feat])[res_feat]
                  .count()
                  .reset_index()
                  .fillna(0))
    newfeature.columns = [grp_feat, feature_name]
    metadata = pd.merge(metadata, newfeature, on=grp_feat, how="outer").fillna(0)
    return metadata

def extract_element_features(metadata, data, element_type, grp_feat):
    """Extract a set of metadata features corresponding to a specific element
    type; centered on unique elements

    Parameters
    ----------
    metadata: pd.DataFrame
        Metadata table
    data: pd.DataFrame
        Original data
    element_type: object
        string designing the element type ("node", "way", or "relation")
    grp_feat: object
        string designing the grouping feature; it characterizes the metadata
    ("chgset", or "user")

    """
    typed_data = data.query('elem==@element_type')
    metadata = create_unique_features(metadata, element_type,
                               typed_data.query("created and open"),
                                      grp_feat, 'id', "_cr")
    metadata = create_unique_features(metadata, element_type,
                               typed_data.query("created and open and not available"),
                               grp_feat, 'id', "_crwrong")
    normalize_features(metadata, 'n_'+element_type+'_cr')
    metadata = create_unique_features(metadata, element_type,
                               typed_data.query("not created and open"),
                               grp_feat, 'id', "_imp")
    metadata = create_unique_features(metadata, element_type,
                               typed_data.query("not created and open and not available"),
                               grp_feat, 'id', "_impwrong")
    normalize_features(metadata, 'n_'+element_type+'_imp')
    metadata = create_unique_features(metadata, element_type,
                               typed_data.query("not created and not open"),
                               grp_feat, 'id', "_del")
    metadata = create_unique_features(metadata, element_type,
                               typed_data.query("not created and not open and available"),
                               grp_feat, 'id', "_delwrong")
    normalize_features(metadata, 'n_'+element_type+'_del')
    return metadata

def create_unique_features(metadata, element_type, data, grp_feat, res_feat, feature_suffix):
    """Create an additional feature to metadata by counting number of unique
    occurrences in data, for a specific element_type

    Parameters
    ----------
    metadata: pd.DataFrame
        Metadata table
    element_type: object
        string designing the element type ("node", "way", or "relation")
    data: pd.DataFrame
        Original data
    grp_feat: object
        string designing the grouping feature; it characterizes the metadata
    ("chgset", or "user")
    res_feat: object
        string that indicates the measured feature (how many items correspond
    feature_suffix: type
        description

    """
    feature_name = 'n_'+ element_type + feature_suffix
    newfeature = (data.groupby([grp_feat])[res_feat]
                  .nunique()
                  .reset_index()
                  .fillna(0))
    newfeature.columns = [grp_feat, feature_name]
    metadata = pd.merge(metadata, newfeature, on=grp_feat, how="outer").fillna(0)
    return metadata

def extract_features(data, pattern, copy=True):
    """Extract features from data that respect the given string pattern

    Parameters
    ----------
    data: pd.DataFrame
    starting dataframe
    pattern: str
    character string that indicates which column has to be kept
    copy: boolean
    True if a copy of the data has to be returned, false otherwise
    """
    if copy:
        return data[[col for col in data.columns
                     if re.search(pattern, col) is not None]].copy()
    else:
        return data[[col for col in data.columns
                     if re.search(pattern, col) is not None]]

def drop_features(data, pattern, copy=True):
    """Drop features from data that respect the given string pattern

    Parameters
    ----------
    data: pd.DataFrame
    starting dataframe
    pattern: str
    character string that indicates which column has to be dropped
    copy: boolean
    True if a copy of the data has to be returned, false otherwise
    """
    if copy:
        return data[[col for col in data.columns
                     if re.search(pattern, col) is None]].copy()
    else:
        return data[[col for col in data.columns
                     if re.search(pattern, col) is None]]

def normalize_temporal_features(metadata, max_lifespan, timehorizon,
                                duration_feats=['lifespan',
                                               'n_inscription_days',
                                               'n_activity_days']):
    """Transform metadata features that are linked with temporal information

    Parameters
    ----------
    metadata: pd.DataFrame
        Metadata table, must contains temporal features
    timehorizon: pd.TimeDelta
        time horizon, used to normalize the temporal feature
    duration_feats: list of objects
        strings designing the name of the individuals activity duration
    
    """
    metadata[duration_feats[0]] = metadata[duration_feats[0]] / max_lifespan
    metadata[duration_feats[1]] = metadata[duration_feats[1]] / timehorizon
    metadata = ecdf_transform(metadata, duration_feats[2])

def normalize_features(metadata, total_column):
    """Transform values of metadata located in cols columns into percentages of
    values in total_column column, without attempting to metadata structure

    Parameters
    ----------
    metadata: pd.DataFrame
        Metadata table
    total_column: object
        String designing the reference column

    """
    transformed_columns = metadata.columns[metadata.columns.to_series()
                                           .str.contains(total_column)]
    metadata[transformed_columns[1:]] = metadata[transformed_columns].apply(lambda x: (x[1:]/x[0]).fillna(0), axis=1)

def logtransform_feature(metadata, column):
    """Apply a logarithm transformation to the column within
    metadata with; to avoid NaN, the applied operation is f:x->log(1+x)

    Parameters
    ----------
    metadata: pd.DataFrame
        Metadata
    column: object
        string designing the name of the column to transform

    """
    metadata[column] = metadata[column].apply(lambda x: math.log(1+x))
