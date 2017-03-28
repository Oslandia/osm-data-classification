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
    chgset_md.sort_values(by=['uid','opened_at'], inplace=True)
    chgset_md['user_lastchgset'] = chgset_md.groupby('uid')['opened_at'].diff()
    chgset_md['lastmodif_at'] = (osm_elements.groupby('chgset')['ts']
                                 .max()
                                 .reset_index()['ts'])
    chgset_md['duration'] = chgset_md.lastmodif_at - chgset_md.opened_at
    
    ###############################################
    # Analyse of users
    print("Extraction of users metadata...")
    user_md = (osm_elements.groupby('uid')['chgset']
                     .nunique()
                     .reset_index())
    user_md['n_modif_bychgset'] = (chgset_md.groupby('uid')['n_modif']
                                   .median()
                                   .reset_index()['n_modif'])
    user_md['nmax_modif_bychgset'] = (chgset_md.groupby('uid')['n_modif']
                                   .max()
                                   .reset_index()['n_modif'])
    user_md['n_elem_bychgset'] = (chgset_md.groupby('uid')['n_uniqelem']
                                   .median()
                                   .reset_index()['n_uniqelem'])
    user_md['nmax_elem_bychgset'] = (chgset_md.groupby('uid')['n_uniqelem']
                                     .max()
                                     .reset_index()['n_uniqelem'])
    user_md['meantime_between_chgset'] = (chgset_md.groupby('uid')['user_lastchgset']
                                          .apply(lambda x: x.mean())
                                          .reset_index()['user_lastchgset'])
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
    user_md.columns = ['uid', 'n_chgset', 'n_modif_bychgset',
                       'nmax_modif_bychgset', 'n_elem_bychgset',
                       'nmax_elem_bychgset', 'meantime_between_chgset',
                       'n_modif', 'n_nodemodif', 'n_waymodif',
                       'n_relationmodif', 'n_modif_byelem', 'n_modif_bynode',
                       'n_modif_byway', 'n_modif_byrelation']
    user_md['version'] = (contrib_byelem.groupby('uid')['version']
                               .max()
                               .reset_index()['version'])
    user_md = user_md.join(contrib_byelem.loc[contrib_byelem.elem=="node"]
                           .groupby('uid')['version']
                           .max(), on='uid', rsuffix='_bynode').fillna(0)
    user_md = user_md.join(contrib_byelem.loc[contrib_byelem.elem=="way"]
                           .groupby('uid')['version']
                           .max(), on='uid', rsuffix='_byway').fillna(0)
    user_md = user_md.join(contrib_byelem.loc[contrib_byelem.elem=="relation"]
                           .groupby('uid')['version']
                           .max(), on='uid', rsuffix='_byrelation').fillna(0)
    user_md.columns = ['uid', 'n_chgset', 'n_modif_bychgset',
                       'nmax_modif_bychgset', 'n_elem_bychgset',
                       'nmax_elem_bychgset', 'meantime_between_chgset',
                       'n_modif', 'n_nodemodif', 'n_waymodif',
                       'n_relationmodif', 'n_modif_byelem', 'n_modif_bynode',
                       'n_modif_byway', 'n_modif_byrelation',
                       'nmax_modif_byelem', 'nmax_modif_bynode',
                       'nmax_modif_byway', 'nmax_modif_byrelation']

    user_md['first_at'] = (osm_elements.groupby('uid')['ts']
                           .min()
                           .reset_index()['ts'])
    user_md['last_at'] = (osm_elements.groupby('uid')['ts']
                          .max()
                          .reset_index()['ts'])
    user_md['activity'] = user_md.last_at - user_md.first_at

    osm_elements.sort_values(by=['elem','id','version'], inplace=True)
    osmelem_vmax = (osm_elements.groupby(['elem','id'])['version']
                    .max()
                    .reset_index())
    osmelem_vmax.columns = ['elem','id','vmax']
    osm_elements = pd.merge(osm_elements, osmelem_vmax, on=['elem','id'])
    osm_elements['up_to_date'] = osm_elements.version == osm_elements.vmax
    osm_elements['willbe_corr'] = np.logical_and(osm_elements.id[1:] ==
                                              osm_elements.id[:-1],
                                              osm_elements.uid[1:] !=
                                              osm_elements.uid[:-1])
    osm_elements['willbe_corr'] = osm_elements.willbe_corr.shift(-1)
    osm_elements.willbe_corr[-1:] = False
    osm_elements.willbe_corr = osm_elements.willbe_corr.astype('bool')
    osm_elements['willbe_autocorr'] = np.logical_and(osm_elements.id[1:] ==
                                              osm_elements.id[:-1],
                                              osm_elements.uid[1:] ==
                                              osm_elements.uid[:-1])
    osm_elements['willbe_autocorr'] = osm_elements.willbe_autocorr.shift(-1)
    osm_elements.willbe_autocorr[-1:] = False
    osm_elements.willbe_autocorr = osm_elements.willbe_autocorr.astype('bool')
    osm_elements['time_before_nextmodif'] = osm_elements.ts.diff()
    osm_elements['time_before_nextmodif'] = (osm_elements.time_before_nextmodif
                                             .shift(-1))
    osm_elements.loc[osm_elements.up_to_date,['time_before_nextmodif']] = pd.NaT
    osm_elements['time_before_nextcorr'] = osm_elements.time_before_nextmodif
    osm_elements['time_before_nextcorr']=osm_elements.time_before_nextcorr.where(osm_elements.willbe_corr,other=pd.NaT)
    osm_elements['time_before_nextauto'] = osm_elements.time_before_nextmodif
    osm_elements['time_before_nextauto']=osm_elements.time_before_nextauto.where(osm_elements.willbe_autocorr,other=pd.NaT)

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
    # Correct 'nextupd_medtime': set it to 'timehorizon'
    # if visible=True and version=latest

    user_md['n_elem'] = (osm_elements.groupby('uid')['id']
                         .nunique()
                         .reset_index())['id']
    user_md = user_md.join(osm_elements.loc[osm_elements.elem=="node"]
                           .groupby('uid')['id']
                           .nunique(), on='uid', rsuffix='_bynode').fillna(0)
    user_md = user_md.join(osm_elements.loc[osm_elements.elem=="way"]
                           .groupby('uid')['id']
                           .nunique(), on='uid', rsuffix='_byway').fillna(0)
    user_md = user_md.join(osm_elements.loc[osm_elements.elem=="relation"]
                           .groupby('uid')['id']
                           .nunique(), on='uid', rsuffix='_byrelation').fillna(0)
    
    osmelem_last_byuser = (osm_elements.groupby(['elem','id','uid'])['version']
                           .last()
                           .reset_index())
    osmelem_last_byuser = pd.merge(osmelem_last_byuser,
                                   osm_elements[['elem','uid','id','version',
                                                 'visible','up_to_date']],
                                   on=['elem','id','uid','version'])
    lastcontrib_byuser = osm_elements.loc[osm_elements.up_to_date,
                                          ['elem','id','uid']]
    osmelem_last_byuser = pd.merge(osmelem_last_byuser, lastcontrib_byuser,
                                   on=['elem','id'])
    osmelem_last_byuser.columns = ['elem','id','uid','version','visible',
                                   'up_to_date','lastuid']

    # osmelem_lastcontrib = (osm_elements.groupby(['elem','id'])['uid']
    #                 .last()
    #                 .reset_index())
    # osmelem_lastcontrib = pd.merge(osmelem_lastcontrib,
    #                                osm_elements.loc[osm_elements.up_to_date,
    #                                                 ['elem','id','visible']],
    #                                on=['elem','id'])
    osmelem_lastcontrib = (osmelem_last_byuser
                           .loc[osmelem_last_byuser.up_to_date])
    osmelem_lastdel = osmelem_lastcontrib.loc[~(osmelem_lastcontrib.visible)]
    user_md = pd.merge(user_md, (osmelem_lastdel.groupby('uid')['elem']
                                 .count()
                                 .reset_index())
                       , on='uid', how="outer").fillna(0)
    user_md = pd.merge(user_md, (osmelem_lastdel
                                 .loc[osmelem_lastdel.elem=="node"]
                                 .groupby('uid')['elem']
                                 .count()
                                 .reset_index())
                       , on='uid', how="outer").fillna(0)
    user_md = pd.merge(user_md, (osmelem_lastdel
                                 .loc[osmelem_lastdel.elem=="way"]
                                 .groupby('uid')['elem']
                                 .count()
                                 .reset_index())
                       , on='uid', how="outer").fillna(0)
    user_md = pd.merge(user_md, (osmelem_lastdel
                                 .loc[osmelem_lastdel.elem=="relation"]
                                 .groupby('uid')['elem']
                                 .count()
                                 .reset_index())
                       , on='uid', how="outer").fillna(0)
    osmelem_lastupd = osmelem_lastcontrib.loc[osmelem_lastcontrib.visible]
    user_md = pd.merge(user_md, (osmelem_lastupd.groupby('uid')['elem']
                                 .count()
                                 .reset_index())
                       , on='uid', how="outer").fillna(0)
    user_md = pd.merge(user_md, (osmelem_lastupd
                                 .loc[osmelem_lastupd.elem=="node"]
                                 .groupby('uid')['elem']
                                 .count()
                                 .reset_index())
                       , on='uid', how="outer").fillna(0)
    user_md = pd.merge(user_md, (osmelem_lastupd
                                 .loc[osmelem_lastupd.elem=="way"]
                                 .groupby('uid')['elem']
                                 .count()
                                 .reset_index())
                       , on='uid', how="outer").fillna(0)
    user_md = pd.merge(user_md, (osmelem_lastupd
                                 .loc[osmelem_lastupd.elem=="relation"]
                                 .groupby('uid')['elem']
                                 .count()
                                 .reset_index())
                       , on='uid', how="outer").fillna(0)
    user_md.columns = ['uid', 'n_chgset', 'n_modif_bychgset',
                       'nmax_modif_bychgset', 'n_elem_bychgset',
                       'nmax_elem_bychgset', 'meantime_between_chgset',
                       'n_modif', 'n_nodemodif', 'n_waymodif',
                       'n_relationmodif', 'n_modif_byelem', 'n_modif_bynode',
                       'n_modif_byway', 'n_modif_byrelation',
                       'nmax_modif_byelem', 'nmax_modif_bynode',
                       'nmax_modif_byway', 'nmax_modif_byrelation',
                       'first_at', 'last_at', 'activity', 'update_medtime',
                       'corr_medtime', 'autocorr_medtime', 'n_elem',
                       'n_node', 'n_way', 'n_relation', 'n_elem_lastdel',
                       'n_node_lastdel', 'n_way_lastdel', 'n_rel_lastdel',
                       'n_elem_lastupd', 'n_node_lastupd', 'n_way_lastupd',
                       'n_rel_lastupd']

    osmelem_last_byuser['oldcontrib'] = np.logical_and(~(osmelem_last_byuser.up_to_date), osmelem_last_byuser.uid != osmelem_last_byuser.lastuid)
    osmelem_oldcontrib = (osmelem_last_byuser
                                  .loc[osmelem_last_byuser.oldcontrib])
    osmelem_olddel = osmelem_oldcontrib.loc[~(osmelem_oldcontrib.visible)]
    user_md = pd.merge(user_md, (osmelem_olddel.groupby('uid')['elem']
                                 .count()
                                 .reset_index())
                       , on='uid', how="outer").fillna(0)
    user_md = pd.merge(user_md, (osmelem_olddel
                                 .loc[osmelem_olddel.elem=="node"]
                                 .groupby('uid')['elem']
                                 .count()
                                 .reset_index())
                       , on='uid', how="outer").fillna(0)
    user_md = pd.merge(user_md, (osmelem_olddel
                                 .loc[osmelem_olddel.elem=="way"]
                                 .groupby('uid')['elem']
                                 .count()
                                 .reset_index())
                       , on='uid', how="outer").fillna(0)
    user_md = pd.merge(user_md, (osmelem_olddel
                                 .loc[osmelem_olddel.elem=="relation"]
                                 .groupby('uid')['elem']
                                 .count()
                                 .reset_index())
                       , on='uid', how="outer").fillna(0)
    osmelem_oldupd = osmelem_oldcontrib.loc[osmelem_oldcontrib.visible]
    user_md = pd.merge(user_md, (osmelem_oldupd.groupby('uid')['elem']
                                 .count()
                                 .reset_index())
                       , on='uid', how="outer").fillna(0)
    user_md = pd.merge(user_md, (osmelem_oldupd
                                 .loc[osmelem_oldupd.elem=="node"]
                                 .groupby('uid')['elem']
                                 .count()
                                 .reset_index())
                       , on='uid', how="outer").fillna(0)
    user_md = pd.merge(user_md, (osmelem_oldupd
                                 .loc[osmelem_oldupd.elem=="way"]
                                 .groupby('uid')['elem']
                                 .count()
                                 .reset_index())
                       , on='uid', how="outer").fillna(0)
    user_md = pd.merge(user_md, (osmelem_oldupd
                                 .loc[osmelem_oldupd.elem=="relation"]
                                 .groupby('uid')['elem']
                                 .count()
                                 .reset_index())
                       , on='uid', how="outer").fillna(0)
    user_md.columns = ['uid', 'n_chgset', 'n_modif_bychgset',
                       'nmax_modif_bychgset', 'n_elem_bychgset',
                       'nmax_elem_bychgset', 'meantime_between_chgset',
                       'n_modif', 'n_nodemodif', 'n_waymodif',
                       'n_relationmodif', 'n_modif_byelem', 'n_modif_bynode',
                       'n_modif_byway', 'n_modif_byrelation',
                       'nmax_modif_byelem', 'nmax_modif_bynode',
                       'nmax_modif_byway', 'nmax_modif_byrelation',
                       'first_at', 'last_at', 'activity', 'update_medtime',
                       'corr_medtime', 'autocorr_medtime', 'n_elem',
                       'n_node', 'n_way', 'n_relation', 'n_elem_lastdel',
                       'n_node_lastdel', 'n_way_lastdel', 'n_rel_lastdel',
                       'n_elem_lastupd', 'n_node_lastupd', 'n_way_lastupd',
                       'n_rel_lastupd', 'n_elem_olddel',
                       'n_node_olddel', 'n_way_olddel', 'n_rel_olddel',
                       'n_elem_oldupd', 'n_node_oldupd', 'n_way_oldupd',
                       'n_rel_oldupd']

    osmelem_creation = (osm_elements.groupby(['elem','id'])['version']
                        .agg({'first':"first",'last':"last"})
                        .stack()
                        .reset_index())
    osmelem_creation.columns = ['elem','id','step','version']
    osmelem_creation = pd.merge(osmelem_creation,
                                   osm_elements[['elem','uid','id','version',
                                                 'visible','up_to_date']],
                                   on=['elem','id','version'])
    osmelem_cr = osmelem_creation.loc[osmelem_creation.step=='first']
    user_md = pd.merge(user_md, (osmelem_cr.groupby('uid')['version']
                                 .count()
                                 .reset_index()),
                       on='uid', how="outer").fillna(0)
    user_md = pd.merge(user_md, (osmelem_cr.loc[osmelem_cr.elem=="node"]
                                 .groupby('uid')['version']
                                 .count()
                                 .reset_index()),
                       on='uid', how="outer").fillna(0)
    user_md = pd.merge(user_md, (osmelem_cr.loc[osmelem_cr.elem=="way"]
                                 .groupby('uid')['version']
                                 .count()
                                 .reset_index()),
                       on='uid', how="outer").fillna(0)
    user_md = pd.merge(user_md, (osmelem_cr.loc[osmelem_cr.elem=="relation"]
                                 .groupby('uid')['version']
                                 .count()
                                 .reset_index()),
                       on='uid', how="outer").fillna(0)
    osmelem_cr_upd = osmelem_cr.loc[osmelem_cr.up_to_date]
    user_md = pd.merge(user_md, (osmelem_cr_upd.groupby('uid')['version']
                                 .count()
                                 .reset_index()),
                       on='uid', how="outer").fillna(0)
    user_md = pd.merge(user_md, (osmelem_cr_upd.loc[osmelem_cr_upd.elem=="node"]
                                 .groupby('uid')['version']
                                 .count()
                                 .reset_index()),
                       on='uid', how="outer").fillna(0)
    user_md = pd.merge(user_md, (osmelem_cr_upd.loc[osmelem_cr_upd.elem=="way"]
                                 .groupby('uid')['version']
                                 .count()
                                 .reset_index()),
                       on='uid', how="outer").fillna(0)
    user_md = pd.merge(user_md, (osmelem_cr_upd.loc[osmelem_cr_upd.elem=="relation"]
                                 .groupby('uid')['version']
                                 .count()
                                 .reset_index()),
                       on='uid', how="outer").fillna(0)
    osmelem_cr_old = osmelem_cr.loc[~(osmelem_cr.up_to_date)]
    osmelem_last = osmelem_creation.loc[osmelem_creation.step=='last']
    osmelem_cr_old = pd.merge(osmelem_cr_old,
                              osmelem_last[['elem','id','visible']],
                          on=['elem','id'])
    osmelem_cr_mod = osmelem_cr_old.loc[osmelem_cr_old.visible_y]
    user_md = pd.merge(user_md, (osmelem_cr_mod.groupby('uid')['version']
                                 .count()
                                 .reset_index()),
                       on='uid', how="outer").fillna(0)
    user_md = pd.merge(user_md, (osmelem_cr_mod.loc[osmelem_cr_mod.elem=="node"]
                                 .groupby('uid')['version']
                                 .count()
                                 .reset_index()),
                       on='uid', how="outer").fillna(0)
    user_md = pd.merge(user_md, (osmelem_cr_mod.loc[osmelem_cr_mod.elem=="way"]
                                 .groupby('uid')['version']
                                 .count()
                                 .reset_index()),
                       on='uid', how="outer").fillna(0)
    user_md = pd.merge(user_md, (osmelem_cr_mod.loc[osmelem_cr_mod.elem=="relation"]
                                 .groupby('uid')['version']
                                 .count()
                                 .reset_index()),
                       on='uid', how="outer").fillna(0)
    osmelem_cr_del = osmelem_cr_old.loc[~(osmelem_cr_old.visible_y)]
    user_md = pd.merge(user_md, (osmelem_cr_del.groupby('uid')['version']
                                 .count()
                                 .reset_index()),
                       on='uid', how="outer").fillna(0)
    user_md = pd.merge(user_md, (osmelem_cr_del.loc[osmelem_cr_del.elem=="node"]
                                 .groupby('uid')['version']
                                 .count()
                                 .reset_index()),
                       on='uid', how="outer").fillna(0)
    user_md = pd.merge(user_md, (osmelem_cr_del.loc[osmelem_cr_del.elem=="way"]
                                 .groupby('uid')['version']
                                 .count()
                                 .reset_index()),
                       on='uid', how="outer").fillna(0)
    user_md = pd.merge(user_md, (osmelem_cr_del.loc[osmelem_cr_del.elem=="relation"]
                                 .groupby('uid')['version']
                                 .count()
                                 .reset_index()),
                       on='uid', how="outer").fillna(0)
    
    user_md.columns = ['uid', 'n_chgset', 'n_modif_bychgset',
                       'nmax_modif_bychgset', 'n_elem_bychgset',
                       'nmax_elem_bychgset', 'meantime_between_chgset',
                       'n_modif', 'n_nodemodif', 'n_waymodif',
                       'n_relationmodif', 'n_modif_byelem', 'n_modif_bynode',
                       'n_modif_byway', 'n_modif_byrelation',
                       'nmax_modif_byelem', 'nmax_modif_bynode',
                       'nmax_modif_byway', 'nmax_modif_byrelation',
                       'first_at', 'last_at', 'activity', 'update_medtime',
                       'corr_medtime', 'autocorr_medtime', 'n_elem',
                       'n_node', 'n_way', 'n_relation', 'n_elem_lastdel',
                       'n_node_lastdel', 'n_way_lastdel', 'n_rel_lastdel',
                       'n_elem_lastupd', 'n_node_lastupd', 'n_way_lastupd',
                       'n_rel_lastupd', 'n_elem_olddel',
                       'n_node_olddel', 'n_way_olddel', 'n_rel_olddel',
                       'n_elem_oldupd', 'n_node_oldupd', 'n_way_oldupd',
                       'n_rel_oldupd', 'n_elem_cr', 'n_node_cr', 'n_way_cr',
                       'n_relation_cr', 'n_elem_crupd', 'n_node_crupd', 'n_way_crupd',
                       'n_relation_crupd', 'n_elem_crmod', 'n_node_crmod',
                       'n_way_crmod', 'n_relation_crmod', 'n_elem_crdel',
                       'n_node_crdel', 'n_way_crdel', 'n_relation_crdel']

    ###############################################
    # Metadata saving
    print("End of extraction - saving...")
    print(elem_md.info())
    print(chgset_md.info())
    print(user_md.info())
    utils.writeOSMmetadata(elem_md, chgset_md, user_md, datapath, dataset_name)
