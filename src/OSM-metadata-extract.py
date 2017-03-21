# coding: utf-8

"""Extract change set and user metadata, from a history OSM data file
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
    # call the script following
    # format 'python3 <data set name> <save_output?>' (2 args)
    if len(sys.argv) != 3:
        print("Usage: python3 <dataset_name> <save_output: y/n>")
        sys.exit(-1)
    dataset_name = sys.argv[1]
    save_output = True if sys.argv[2]=="y" or sys.argv[2]=="Y" else False
    datapath = "~/data/" + dataset_name + "/"
    osm_elements = pd.read_csv(datapath + "latest-" +
                               dataset_name + "-elements.csv",
                               index_col=0, parse_dates=['ts'])

        ###############################################
    # Analyse of elements
    userbyelem = osm_elements.groupby(
        ['elem','id'])['uid'].nunique().reset_index()
    versionbyelem = osm_elements.groupby(
        ['elem','id'])['version'].count().reset_index()
    elembegin = osm_elements.groupby(['elem','id'])['ts'].min().reset_index()
    elembegin.columns = ['elem','id','created_at']
    elemend = osm_elements.groupby(['elem','id'])['ts'].max().reset_index()
    elemend.columns = ['elem','id','lastmodif_at']
    elemsynthesis = pd.merge(userbyelem, versionbyelem, on=['elem','id'])
    elemsynthesis = pd.merge(elemsynthesis, elembegin, on=['elem','id'])
    elemsynthesis = pd.merge(elemsynthesis, elemend, on=['elem','id'])
    elemsynthesis['available'] = np.repeat([True], len(elemsynthesis))
    mask = np.logical_and(elemsynthesis.elem=="n",
                          elemsynthesis.id.isin(nodes.loc[nodes.visible==False
                                                          ,"id"]))
    elemsynthesis.loc[mask,"available"] = False
    mask = np.logical_and(elemsynthesis.elem=="w",
                          elemsynthesis.id.isin(ways.loc[ways.visible==False
                                                         ,"id"]))
    elemsynthesis.loc[mask,"available"] = False
    mask = np.logical_and(elemsynthesis.elem=="r",
                          elemsynthesis.id.isin(relations.loc[
                              relations.visible==False,"id"]))
    elemsynthesis.loc[mask,"available"] = False
    elemsynthesis['lifecycle'] = (elemsynthesis.lastmodif_at
                                  - elemsynthesis.created_at)
    timehorizon = pd.to_datetime("2017-02-13 00:00:00")
    elemsynthesis.loc[elemsynthesis.available==True,"lifecycle"] = timehorizon - elemsynthesis.loc[elemsynthesis.available==True,"created_at"]
    elemsynthesis.loc[elemsynthesis.available.values,"lifecycle"] = timehorizon - elemsynthesis.loc[elemsynthesis.available.values,"created_at"]

    ###############################################
    # Analyse of change sets
    chgsetsynthesis = (osm_elements.groupby(['chgset'])['elem']
                       .count().reset_index())
    chgsetsynthesis = pd.merge(chgsetsynthesis,
                               osm_elements.loc[:,['chgset','uid']]
                               .drop_duplicates(), on=['chgset'])
    nodemodifbychgset = (osm_elements.loc[osm_elements.elem=="n"]
                         .groupby(['chgset'])['elem'].count().reset_index())
    nodemodifbychgset.columns = ['chgset','nnode']
    chgsetsynthesis = pd.merge(chgsetsynthesis, nodemodifbychgset,
                               how="left", on=['chgset'])
    chgsetsynthesis.nnode[np.isnan(chgsetsynthesis.nnode)] = 0
    waymodifbychgset = (osm_elements.loc[osm_elements.elem=="w"]
                        .groupby(['chgset'])['elem'].count().reset_index())
    waymodifbychgset.columns = ['chgset','nway']
    chgsetsynthesis = pd.merge(chgsetsynthesis, waymodifbychgset,
                               how="left", on=['chgset'])
    chgsetsynthesis.nway[np.isnan(chgsetsynthesis.nway)] = 0
    relationmodifbychgset = (osm_elements.loc[osm_elements.elem=="r"]
                             .groupby(['chgset'])['elem'].count().reset_index())
    relationmodifbychgset.columns = ['chgset','nrelation']
    chgsetsynthesis = pd.merge(chgsetsynthesis, relationmodifbychgset,
                               how="left", on=['chgset'])
    chgsetsynthesis.nrelation[np.isnan(chgsetsynthesis.nrelation)] = 0
    chgsetbegin = osm_elements.groupby(['chgset'])['ts'].min().reset_index()
    chgsetbegin.columns = ['chgset','opened_at']
    chgsetend = osm_elements.groupby(['chgset'])['ts'].max().reset_index()
    chgsetend.columns = ['chgset','lastmodif_at']
    chgsetsynthesis = pd.merge(chgsetsynthesis, chgsetbegin, on=['chgset'])
    chgsetsynthesis = pd.merge(chgsetsynthesis, chgsetend, on=['chgset'])
    chgsetsynthesis['duration'] = (chgsetsynthesis.lastmodif_at
                                   - chgsetsynthesis.opened_at)
    
    ###############################################
    # Analyse of users
    usersynthesis = (osm_elements.groupby(['uid'])['chgset']
                     .nunique().reset_index())
    elembyuser = osm_elements.groupby(['uid'])['elem'].count().reset_index()
    usersynthesis = pd.merge(usersynthesis, elembyuser, on=['uid'])
    nodemodifbyuser = (osm_elements.loc[osm_elements.elem=="n"]
                       .groupby(['uid'])['elem'].count().reset_index())
    nodemodifbyuser.columns = ['uid','nnode']
    usersynthesis = pd.merge(usersynthesis, nodemodifbyuser,
                             how="left", on=['uid'])
    usersynthesis.nnode[np.isnan(usersynthesis.nnode)] = 0
    waymodifbyuser = (osm_elements.loc[osm_elements.elem=="w"]
                      .groupby(['uid'])['elem'].count().reset_index())
    waymodifbyuser.columns = ['uid','nway']
    usersynthesis = pd.merge(usersynthesis, waymodifbyuser,
                             how="left", on=['uid'])
    usersynthesis.nway[np.isnan(usersynthesis.nway)] = 0
    relationmodifbyuser = (osm_elements.loc[osm_elements.elem=="r"]
                           .groupby(['uid'])['elem'].count().reset_index())
    relationmodifbyuser.columns = ['uid','nrelation']
    usersynthesis = pd.merge(usersynthesis, relationmodifbyuser,
                             how="left", on=['uid'])
    usersynthesis.nrelation[np.isnan(usersynthesis.nrelation)] = 0
    contribbyuserelem = (osm_elements.groupby(['elem','id','uid'])
                         .size().reset_index())
    nmodifperuserperelem = (contribbyuserelem.groupby(['uid'])[0]
                            .median().reset_index())
    nmodifperuserperelem.columns = ['uid','nmodifperelem']
    usersynthesis = pd.merge(usersynthesis, nmodifperuserperelem,
                             how="left", on=['uid'])
    nnodemodifperuserperelem = contribbyuserelem.loc[contribbyuserelem.elem=="n"].groupby(['uid'])[0].median().reset_index()
    nnodemodifperuserperelem.columns = ['uid','nnodemodifperelem']
    usersynthesis = pd.merge(usersynthesis, nnodemodifperuserperelem,
                             how="left", on=['uid'])
    usersynthesis.nnodemodifperelem[np.isnan(usersynthesis.nnodemodifperelem)] = 0
    nwaymodifperuserperelem = contribbyuserelem.loc[contribbyuserelem.elem=="w"].groupby(['uid'])[0].median().reset_index()
    nwaymodifperuserperelem.columns = ['uid','nwaymodifperelem']
    usersynthesis = pd.merge(usersynthesis, nwaymodifperuserperelem,
                             how="left", on=['uid'])
    usersynthesis.nwaymodifperelem[np.isnan(usersynthesis.nwaymodifperelem)] = 0
    nrelationmodifperuserperelem = contribbyuserelem.loc[contribbyuserelem.elem=="r"].groupby(['uid'])[0].median().reset_index()
    nrelationmodifperuserperelem.columns = ['uid','nrelationmodifperelem']
    usersynthesis = pd.merge(usersynthesis, nrelationmodifperuserperelem,
                             how="left", on=['uid'])
    usersynthesis.nrelationmodifperelem[np.isnan(usersynthesis.nrelationmodifperelem)] = 0
    userbegin = osm_elements.groupby(['uid'])['ts'].min().reset_index()
    userbegin.columns = ['uid','first_at']
    userend = osm_elements.groupby(['uid'])['ts'].max().reset_index()
    userend.columns = ['uid','last_at']
    usersynthesis = pd.merge(usersynthesis, userbegin, on=['uid'])
    usersynthesis = pd.merge(usersynthesis, userend, on=['uid'])
    usersynthesis['activity'] = usersynthesis.last_at - usersynthesis.first_at

    print(elemsynthesis.info())
    print(chgsetsynthesis.info())
    print(usersynthesis.info())
    
    ###############################################
    # Metadata saving
    if(save_output):
        utils.writeOSMmetadata(elemsynthesis, chgsetsynthesis,
                               usersynthesis, dataset_name)
