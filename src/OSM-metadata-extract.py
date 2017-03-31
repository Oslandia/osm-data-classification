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

    ###############################################
    # Analyse of elements
    print("Extraction of elements metadata...")
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
    print("Extraction of change sets metadata...")
    chgset_md = (osm_elements.groupby(['chgset', 'uid'])['elem']
                 .count().reset_index())
    chgset_md_byelem = (osm_elements.groupby(['chgset','elem'])['elem']
                        .count()
                        .unstack()
                        .reset_index()
                        .fillna(0))
    chgset_md = pd.merge(chgset_md, chgset_md_byelem, on='chgset')
    chgset_md.columns.values[-4:] = ['n_modif', 'n_nodemodif',
                                     'n_relationmodif', 'n_waymodif']
    modif_bychgset = (osm_elements.groupby(['elem', 'id', 'chgset'])['version']
                      .count()
                      .reset_index())
    chgset_md['version'] = (modif_bychgset.groupby(['chgset'])['id']
                            .nunique()
                            .reset_index())['id']
    chgset_md_byelem = (osm_elements.groupby(['chgset','elem'])['id']
                        .nunique()
                        .unstack()
                        .reset_index()
                        .fillna(0))
    chgset_md = pd.merge(chgset_md, chgset_md_byelem, on='chgset')
    chgset_md.columns.values[-4:] = ['n_uniqelem', 'n_uniqnode',
                                     'n_uniqrelation', 'n_uniqway']
    del chgset_md_byelem
    chgset_md['opened_at'] = (osm_elements.groupby('chgset')['ts']
                              .min()
                              .reset_index()['ts'])
    chgset_md.sort_values(by=['uid','opened_at'], inplace=True)
    chgset_md['user_lastchgset'] = chgset_md.groupby('uid')['opened_at'].diff()
    chgset_md['lastmodif_at'] = (osm_elements.groupby('chgset')['ts']
                                 .max()
                                 .reset_index()['ts'])
    chgset_md['duration'] = chgset_md.lastmodif_at - chgset_md.opened_at

    ########################################
    # Add some features to OSM elements (needed for user metadata)
    osm_elements.sort_values(by=['elem','id','version'], inplace=True)
    # Maximal version of each elements
    osmelem_vmax = (osm_elements.groupby(['elem','id'])['version']
                    .max()
                    .reset_index())
    osmelem_vmax.columns = ['elem','id','vmax']
    osm_elements = pd.merge(osm_elements, osmelem_vmax, on=['elem','id'])
    # Whether or not an elements is up-to-date
    osm_elements['up_to_date'] = osm_elements.version == osm_elements.vmax
    # Whether or not an element will be corrected by another user
    osm_elements['willbe_corr'] = np.logical_and(osm_elements.id[1:] ==
                                              osm_elements.id[:-1],
                                              osm_elements.uid[1:] !=
                                              osm_elements.uid[:-1])
    osm_elements['willbe_corr'] = osm_elements.willbe_corr.shift(-1)
    osm_elements.willbe_corr[-1:] = False
    osm_elements.willbe_corr = osm_elements.willbe_corr.astype('bool')
    # Whether or not an element will be corrected by the same user
    osm_elements['willbe_autocorr'] = np.logical_and(osm_elements.id[1:] ==
                                              osm_elements.id[:-1],
                                              osm_elements.uid[1:] ==
                                              osm_elements.uid[:-1])
    osm_elements['willbe_autocorr'] = osm_elements.willbe_autocorr.shift(-1)
    osm_elements.willbe_autocorr[-1:] = False
    osm_elements.willbe_autocorr = osm_elements.willbe_autocorr.astype('bool')
    # Time before the next modification
    osm_elements['time_before_nextmodif'] = osm_elements.ts.diff()
    osm_elements['time_before_nextmodif'] = (osm_elements.time_before_nextmodif
                                             .shift(-1))
    osm_elements.loc[osm_elements.up_to_date,['time_before_nextmodif']] = pd.NaT
    # Time before the next modification, if it is done by another user
    osm_elements['time_before_nextcorr'] = osm_elements.time_before_nextmodif
    osm_elements['time_before_nextcorr']=osm_elements.time_before_nextcorr.where(osm_elements.willbe_corr,other=pd.NaT)
    # Time before the next modification, if it is done by the same user
    osm_elements['time_before_nextauto'] = osm_elements.time_before_nextmodif
    osm_elements['time_before_nextauto']=osm_elements.time_before_nextauto.where(osm_elements.willbe_autocorr,other=pd.NaT)
        
    ###############################################
    # Analyse of users
    print("Extraction of users metadata...")
    user_md = (osm_elements.groupby('uid')['chgset']
                     .nunique()
                     .reset_index())
    user_md = pd.merge(user_md, (chgset_md
                                 .groupby('uid')['n_modif']
                                 .agg({'nmed_modif_bychgset':"median",
                                       'nmax_modif_bychgset':"max"})
                                 .reset_index()))
    user_md = pd.merge(user_md, (chgset_md
                                 .groupby('uid')['n_uniqelem']
                                 .agg({'nmed_elem_bychgset':"median",
                                       'nmax_elem_bychgset':"max"})
                                 .reset_index()))
    user_md['meantime_between_chgset'] = (chgset_md
                                          .groupby('uid')['user_lastchgset']
                                          .apply(lambda x: x.mean())
                                          .reset_index()['user_lastchgset'])
    #
    user_md['n_modif'] = (osm_elements.groupby('uid')['elem']
                          .count()
                          .reset_index()['elem'])
    user_md_byelem = (osm_elements.groupby(['uid','elem'])['id']
                      .nunique()
                      .unstack()
                      .reset_index()
                      .fillna(0))
    user_md = pd.merge(user_md, user_md_byelem, on='uid')
    user_md.columns.values[-4:] = ['n_modif', 'n_nodemodif',
                                     'n_relationmodif', 'n_waymodif']
    #
    contrib_byelem = (osm_elements.groupby(['elem', 'id', 'uid'])['version']
                      .count()
                      .reset_index())
    user_md['nmed_modif_byelem'] = (contrib_byelem.groupby('uid')['version']
                                 .median()
                                 .reset_index()['version'])
    user_md_byelem = (contrib_byelem.groupby(['uid','elem'])['version']
                      .median()
                      .unstack()
                      .reset_index()
                      .fillna(0))
    user_md = pd.merge(user_md, user_md_byelem, on='uid')
    user_md.columns.values[-3:] = ['nmed_modif_bynode',
                                   'nmed_modif_byrelation',
                                   'nmed_modif_byway']
    user_md_byelem = (contrib_byelem.groupby(['uid','elem'])['version']
                      .max()
                      .unstack()
                      .reset_index()
                      .fillna(0))
    user_md = pd.merge(user_md, user_md_byelem, on='uid')
    user_md.columns.values[-3:] = ['nmax_modif_bynode',
                                   'nmax_modif_byrelation',
                                   'nmax_modif_byway']
    #
    user_md['first_at'] = (osm_elements.groupby('uid')['ts']
                           .min()
                           .reset_index()['ts'])
    user_md['last_at'] = (osm_elements.groupby('uid')['ts']
                          .max()
                          .reset_index()['ts'])
    user_md['activity'] = user_md.last_at - user_md.first_at
    #
    user_md['update_medtime'] = (osm_elements
                                  .groupby('uid')['time_before_nextmodif']
                                  .apply(lambda x: x.median())
                                  .reset_index()['time_before_nextmodif'])
    user_md['corr_medtime'] = (osm_elements
                                  .groupby('uid')['time_before_nextcorr']
                                  .apply(lambda x: x.median())
                                  .reset_index()['time_before_nextcorr'])
    user_md['autocorr_medtime'] = (osm_elements
                                  .groupby('uid')['time_before_nextauto']
                                  .apply(lambda x: x.median())
                                  .reset_index()['time_before_nextauto'])
    #
    user_md_byelem = (osm_elements.groupby(['uid','elem'])['id']
                      .nunique()
                      .unstack()
                      .reset_index()
                      .fillna(0))
    user_md_byelem['n_elem'] = (user_md_byelem[['node','relation','way']]
                                .apply(sum, axis=1))
    user_md = pd.merge(user_md, user_md_byelem, on='uid')
    user_md.columns.values[-4:] = ['n_node', 'n_relation', 'n_way', 'n_elem']
    #    
    osmelem_last_byuser = (osm_elements.groupby(['elem','id','uid'])['version']
                           .last()
                           .reset_index())
    osmelem_last_byuser = pd.merge(osmelem_last_byuser,
                                   osm_elements[['elem','uid','id','version',
                                                 'visible','up_to_date']],
                                   on=['elem','id','uid','version'])
    lastcontrib_byuser = osm_elements.query("up_to_date")[['elem','id','uid']]
    osmelem_last_byuser = pd.merge(osmelem_last_byuser, lastcontrib_byuser,
                                   on=['elem','id'])
    osmelem_last_byuser.columns = ['elem','id','uid','version','visible',
                                   'up_to_date','lastuid']
    #
    osmelem_lastcontrib = osmelem_last_byuser.query("up_to_date")
    osmelem_lastdel = osmelem_lastcontrib.query("not visible")
    user_md_byelem = (osmelem_lastdel.groupby(['uid','elem'])['id']
                      .count()
                      .unstack()
                      .reset_index()
                      .fillna(0))
    user_md_byelem['elem'] = (user_md_byelem[['node','relation','way']]
                                .apply(sum, axis=1))
    user_md = pd.merge(user_md, user_md_byelem, on='uid')
    user_md.columns.values[-4:] = ['n_node_lastdel', 'n_relation_lastdel',
                                   'n_way_lastdel', 'n_elem_lastdel']
    #    
    osmelem_lastupd = osmelem_lastcontrib.query("visible")
    user_md_byelem = (osmelem_lastupd.groupby(['uid','elem'])['id']
                      .count()
                      .unstack()
                      .reset_index()
                      .fillna(0))
    user_md_byelem['elem'] = (user_md_byelem[['node','relation','way']]
                                .apply(sum, axis=1))
    user_md = pd.merge(user_md, user_md_byelem, on='uid')
    user_md.columns.values[-4:] = ['n_node_lastupd', 'n_relation_lastupd',
                                   'n_way_lastupd', 'n_elem_lastupd']
    #
    osmelem_last_byuser['oldcontrib'] = np.logical_and(
        ~(osmelem_last_byuser.up_to_date),
        osmelem_last_byuser.uid != osmelem_last_byuser.lastuid)
    osmelem_oldcontrib = osmelem_last_byuser.query("old_contrib")
    osmelem_olddel = osmelem_oldcontrib.query("not visible")
    user_md_byelem = (osmelem_olddel.groupby(['uid','elem'])['id']
                      .count()
                      .unstack()
                      .reset_index()
                      .fillna(0))
    user_md_byelem['elem'] = (user_md_byelem[['node','relation','way']]
                                .apply(sum, axis=1))
    user_md = pd.merge(user_md, user_md_byelem, on='uid')
    user_md.columns.values[-4:] = ['n_node_olddel', 'n_relation_olddel',
                                   'n_way_olddel', 'n_elem_olddel']
    #
    osmelem_oldupd = osmelem_oldcontrib.query("visible")
    user_md_byelem = (osmelem_oldupd.groupby(['uid','elem'])['id']
                      .count()
                      .unstack()
                      .reset_index()
                      .fillna(0))
    user_md_byelem['elem'] = (user_md_byelem[['node','relation','way']]
                                .apply(sum, axis=1))
    user_md = pd.merge(user_md, user_md_byelem, on='uid')
    user_md.columns.values[-4:] = ['n_node_oldupd', 'n_relation_oldupd',
                                   'n_way_oldupd', 'n_elem_oldupd']
    #
    osmelem_creation = (osm_elements.groupby(['elem','id'])['version']
                        .agg({'first':"first",'last':"last"})
                        .stack()
                        .reset_index())
    osmelem_creation.columns = ['elem','id','step','version']
    osmelem_creation = pd.merge(osmelem_creation,
                                   osm_elements[['elem','uid','id','version',
                                                 'visible','up_to_date']],
                                   on=['elem','id','version'])
    #
    osmelem_cr = osmelem_creation.query("step == 'first'")
    user_md_byelem = (osmelem_cr.groupby(['uid','elem'])['id']
                      .count()
                      .unstack()
                      .reset_index()
                      .fillna(0))
    user_md_byelem['elem'] = (user_md_byelem[['node','relation','way']]
                                .apply(sum, axis=1))
    user_md = pd.merge(user_md, user_md_byelem, on='uid')
    user_md.columns.values[-4:] = ['n_node_cr', 'n_relation_cr',
                                   'n_way_cr', 'n_elem_cr']
    #
    osmelem_cr_upd = osmelem_cr.loc[osmelem_cr.up_to_date]
    user_md_byelem = (osmelem_cr_upd.groupby(['uid','elem'])['id']
                      .count()
                      .unstack()
                      .reset_index()
                      .fillna(0))
    user_md_byelem['elem'] = (user_md_byelem[['node','relation','way']]
                                .apply(sum, axis=1))
    user_md = pd.merge(user_md, user_md_byelem, on='uid')
    user_md.columns.values[-4:] = ['n_node_crupd', 'n_relation_crupd',
                                   'n_way_crupd', 'n_elem_crupd']
    #
    osmelem_cr_old = osmelem_cr.query("not up_to_date")
    osmelem_last = osmelem_creation.query("step == 'last'") 
    osmelem_cr_old = pd.merge(osmelem_cr_old,
                              osmelem_last[['elem','id','visible']],
                          on=['elem','id'])
    osmelem_cr_mod = osmelem_cr_old.query("visible_y")
    user_md_byelem = (osmelem_cr_mod.groupby(['uid','elem'])['id']
                      .count()
                      .unstack()
                      .reset_index()
                      .fillna(0))
    user_md_byelem['elem'] = (user_md_byelem[['node','relation','way']]
                                .apply(sum, axis=1))
    user_md = pd.merge(user_md, user_md_byelem, on='uid')
    user_md.columns.values[-4:] = ['n_node_crmod', 'n_relation_crmod',
                                   'n_way_crmod', 'n_elem_crmod']
    #
    osmelem_cr_del = osmelem_cr_old.query("not visible_y")
    user_md_byelem = (osmelem_cr_del.groupby(['uid','elem'])['id']
                      .count()
                      .unstack()
                      .reset_index()
                      .fillna(0))
    user_md_byelem['elem'] = (user_md_byelem[['node','relation','way']]
                                .apply(sum, axis=1))
    user_md = pd.merge(user_md, user_md_byelem, on='uid')
    user_md.columns.values[-4:] = ['n_node_crdel', 'n_relation_crdel',
                                   'n_way_crdel', 'n_elem_crdel']

    ###############################################
    # Metadata saving
    print("End of extraction - saving...")
    print(elem_md.info())
    print(chgset_md.info())
    print(user_md.info())
    utils.writeOSMmetadata(elem_md, chgset_md, user_md, datapath, dataset_name)
