# coding: utf-8

"""
Extract change set and user metadata, from a history OSM data file
The file has to be runned through the following format:
python <pathto_OSM-metadata-extract.py> <pathto_csvfiles> <dataset_name>
"""

###############################################
# Import packages #############################
###############################################
import sys
import os.path as osp
from datetime import datetime
if sys.version_info[0] == 3:
    from datetime import timezone

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import utils

###############################################
# Main method #################################
###############################################
if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python <pathto_OSM-metadata-extract.py> <pathto_csvfiles> <dataset_name>")
        sys.exit(-1)
    datapath = sys.argv[1]
    dataset_name = sys.argv[2]
    osm_elements = pd.read_csv(datapath + "/" + dataset_name + "-elements.csv",
                               index_col=0, parse_dates=['ts'])
    nodes = pd.read_csv(datapath + "/" + dataset_name + "-nodes.csv",
                        index_col=0, parse_dates=['ts'])
    ways = pd.read_csv(datapath + "/" + dataset_name + "-ways.csv",
                        index_col=0, parse_dates=['ts'])
    relations = pd.read_csv(datapath + "/" + dataset_name + "-relations.csv",
                        index_col=0, parse_dates=['ts'])

        ###############################################
    # Analyse of elements
    elem_md = (osm_elements.groupby(['elem', 'id'])['version']
               .max()
               .reset_index())
    elem_md = pd.merge(elem_md, osm_elements[['elem','id','version','visible']],
                       on=['elem','id','version'])
    elem_md['n_user'] = (osm_elements.groupby(['elem', 'id'])['uid']
                         .nunique()
                         .reset_index())['uid']
    elem_md['n_chgset'] = (osm_elements.groupby(['elem', 'id'])['chgset']
                         .nunique()
                         .reset_index())['chgset']
    elem_md['created_at'] = (osm_elements.groupby(['elem', 'id'])['ts']
                             .min()
                             .reset_index()['ts'])
    elem_md['lastmodif_at'] = (osm_elements.groupby(['elem', 'id'])['ts']
                               .max()
                               .reset_index()['ts'])
    elem_md['lifecycle'] = elem_md.lastmodif_at - elem_md.created_at
    timehorizon = pd.to_datetime("2017-02-13 00:00:00")
    elem_md.loc[elem_md.visible.values, "lifecycle"] = timehorizon - elem_md.loc[elem_md.visible.values, "created_at"]
    elem_md['mntime_between_modif'] = (elem_md.lifecycle / elem_md.version).astype('timedelta64[m]')

    ###############################################
    # Analyse of change sets
    chgset_md = (osm_elements.groupby(['chgset', 'uid'])['elem']
                 .count().reset_index())
    chgset_md = chgset_md.join(osm_elements.loc[osm_elements.elem=="node"]
                               .groupby('chgset')['elem']
                               .count(), on='chgset', rsuffix='_node').fillna(0)
    chgset_md = chgset_md.join(osm_elements.loc[osm_elements.elem=="way"]
                               .groupby('chgset')['elem']
                               .count(), on='chgset', rsuffix='_way').fillna(0)
    chgset_md = chgset_md.join(osm_elements.loc[osm_elements.elem=="relation"]
                               .groupby('chgset')['elem']
                               .count(), on='chgset', rsuffix='_relation').fillna(0)
    modif_bychgset = (osm_elements.groupby(['elem', 'id', 'chgset'])['version']
                      .count()
                      .reset_index())
    chgset_md['version'] = (modif_bychgset.groupby(['chgset'])['id']
                            .nunique()
                            .reset_index())['id']
    chgset_md = chgset_md.join(modif_bychgset.loc[modif_bychgset.elem=="node"]
                               .groupby('chgset')['id']
                               .nunique(), on='chgset', rsuffix='_byn').fillna(0)
    chgset_md = chgset_md.join(modif_bychgset.loc[modif_bychgset.elem=="way"]
                           .groupby('chgset')['id']
                           .nunique(), on='chgset', rsuffix='_byw').fillna(0)
    chgset_md = chgset_md.join(modif_bychgset.loc[modif_bychgset.elem=="relation"]
                           .groupby('chgset')['id']
                           .nunique(), on='chgset', rsuffix='_byr').fillna(0)
    chgset_md.columns = ['chgset', 'uid', 'n_modif', 'n_nodemodif',
                         'n_waymodif', 'n_relationmodif', 'n_uniqelem',
                         'n_uniqnode', 'n_uniqway', 'n_uniqrelation']
    
    chgset_md['opened_at'] = (osm_elements.groupby('chgset')['ts']
                              .min()
                              .reset_index()['ts'])
    chgset_md['lastmodif_at'] = (osm_elements.groupby('chgset')['ts']
                                 .max()
                                 .reset_index()['ts'])
    chgset_md['duration'] = chgset_md.lastmodif_at - chgset_md.opened_at
    
    ###############################################
    # Analyse of users
    user_md = (osm_elements.groupby('uid')['chgset']
                     .nunique()
                     .reset_index())
    user_md['elem'] = (osm_elements.groupby('uid')['elem']
                       .count()
                       .reset_index()['elem'])
    user_md = user_md.join(osm_elements.loc[osm_elements.elem=="node"]
                               .groupby('uid')['elem']
                               .count(), on='uid', rsuffix='_node').fillna(0)
    user_md = user_md.join(osm_elements.loc[osm_elements.elem=="way"]
                               .groupby('uid')['elem']
                               .count(), on='uid', rsuffix='_way').fillna(0)
    user_md = user_md.join(osm_elements.loc[osm_elements.elem=="relation"]
                               .groupby('uid')['elem']
                               .count(), on='uid', rsuffix='_relation').fillna(0)

    contrib_byelem = (osm_elements.groupby(['elem', 'id', 'uid'])['version']
                      .count()
                      .reset_index())
    user_md['version'] = (contrib_byelem.groupby('uid')['version']
                               .median()
                               .reset_index()['version'])
    user_md = user_md.join(contrib_byelem.loc[contrib_byelem.elem=="node"]
                           .groupby('uid')['version']
                           .median(), on='uid', rsuffix='_bynode').fillna(0)
    user_md = user_md.join(contrib_byelem.loc[contrib_byelem.elem=="way"]
                           .groupby('uid')['version']
                           .median(), on='uid', rsuffix='_byway').fillna(0)
    user_md = user_md.join(contrib_byelem.loc[contrib_byelem.elem=="relation"]
                           .groupby('uid')['version']
                           .median(), on='uid', rsuffix='_byrelation').fillna(0)
    user_md.columns = ['uid', 'n_chgset', 'n_modif', 'n_nodemodif',
                       'n_waymodif', 'n_relationmodif', 'n_modif_byelem',
                       'n_modif_bynode', 'n_modif_byway', 'n_modif_byrelation']

    user_md['first_at'] = (osm_elements.groupby('uid')['ts']
                           .min()
                           .reset_index()['ts'])
    user_md['last_at'] = (osm_elements.groupby('uid')['ts']
                          .max()
                          .reset_index()['ts'])
    user_md['activity'] = user_md.last_at - user_md.first_at

    ###############################################
    # Metadata saving
    print(elem_md.info())
    print(chgset_md.info())
    print(user_md.info())
    utils.writeOSMmetadata(elem_md, chgset_md, user_md, datapath, dataset_name)
