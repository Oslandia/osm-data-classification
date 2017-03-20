# coding: utf-8

"""Extract some stats for OSM nodes, from a history OSM data file
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
    print("Analyse of the OSM data gathered for {0}".format(dataset_name))
    if(save_output):
        print("Outputs will be saved into .csv files during process!")
    # Data reading
    tlnodes,tlways,tlrelations = utils.readOSMdata(dataset_name)
    print("There are {0} feature(s) and {1} individual(s) in node dataframe"
          .format(len(tlnodes.columns),len(tlnodes)))
    print("There are {0} feature(s) and {1} individual(s) in way dataframe"
          .format(len(tlways.columns),len(tlways)))
    print("There are {0} feature(s) and {1} individual(s) in relation dataframe"
          .format(len(tlrelations.columns),len(tlrelations)))

    ###############################################
    # Build last OSM elements starting from history data
    # and save them into dedicated files
    nodes = utils.updatedelem(tlnodes)
    ways = utils.updatedelem(tlways)
    relations = utils.updatedelem(tlrelations)
    if(save_output):
        utils.writeOSMdata(nodes,ways,relations,dataset_name)
    
    # Gather every elements into a single data frame
    elem = pd.Series(np.repeat(["n", "w", "r"]
                               , [len(tlnodes), len(tlways), len(tlrelations)]
                               , axis=0)
                     , name="elem").astype("category")
    osm_elements = pd.concat([tlnodes.loc[:,['id','version','visible','ts','uid'
                                             ,'chgset','ntags','tagkeys']],
                              tlways.loc[:,['id','version','visible','ts','uid'
                                            ,'chgset','ntags','tagkeys']],
                              tlrelations.loc[:,['id','version','visible','ts'
                                                 ,'uid','chgset','ntags'
                                                 ,'tagkeys']]]
                             , ignore_index=True)
    osm_elements = pd.concat([elem, osm_elements], axis=1)
    osm_elements = osm_elements.sort_values(by=['ts'])

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

    ###############################################
    # Metadata saving
    if(save_output):
        utils.writeOSMmetadata(elemsynthesis, chgsetsynthesis,
                               usersynthesis, dataset_name)

    ###############################################
    # Plot metadata: number of unique contributors per element
    print("Plot 1: number of contributors per element...")
    y = np.linspace(0,1,1001)
    utils.single_empCDF(elemsynthesis.uid.quantile(y), y, lims=[0,20,0,1],
                        xlab="Number of unique contributors per OSM element",
                        ylab="Empirical CDF",
                        figpath="figs/uniquserperelem.png")
    print("Plot 2: number of contributors per element and per type...")
    x = elemsynthesis.groupby('elem')['uid'].quantile(y).unstack(0)
    utils.multi_empCDF(x, y, lims=[0,20,0,1], legend=True,
                       xlab="Number of unique contributors per OSM element",
                       ylab="Empirical CDF",
                       figpath="figs/uniquserperelemtype.png")

    # Plot metadata: number of versions per element
    print("Plot 3: number of versions per element...")
    utils.single_empCDF(elemsynthesis.version.quantile(y), y, lims=[0,20,0,1],
                        xlab="Number of versions per OSM element",
                        ylab="Empirical CDF",
                        figpath="figs/nbversionperelem.png")
    print("Plot 4: number of versions per element and per type...")
    x = elemsynthesis.groupby('elem')['version'].quantile(y).unstack(0)
    utils.multi_empCDF(x, y, lims=[0,80,0,1], legend=True,
                       xlab="Number of versions per OSM element",
                       ylab="Empirical CDF",
                       figpath="figs/nbversionperelemtype.png")

    # Plot metadata: element global description
    print("Plot 5: element global description...")
    utils.condsctrplot('uid', 'version', elemsynthesis.groupby('elem'),
                       lims=[0,150,0,600], bsctr=True, mkr="+", legend=True,
                       figpath="figs/elem_uniquservsversion.png",
                       xlab="Number of unique users per OSM element",
                       ylab="Number of version per OSM element")

    # Plot metadata: number of modifications per change set
    print("Plot 6: number of contributors per element...")
    utils.single_empCDF(chgsetsynthesis.elem.quantile(y), y, lims=[0,200,0,1],
                        hltqt=0.9, ylab="Empirical CDF",
                        xlab="Number of modifications per change set",
                        figpath="figs/nbmodifperchgset.png")
    print("Plot 7: number of contributors per element and per type...")
    x = chgsetsynthesis.loc[:,['nnode','nway','nrelation']].apply(
        lambda x: x.quantile(y))
    utils.multi_empCDF(x, y, lims=[0,100,0,1], hltqt=0.9, legend=True,
                       xlab="Number of modifications per change set",
                       ylab="Empirical CDF",
                       figpath="figs/nbmodifperchgsettype.png")
    
    # Plot metadata: change set global description
    print("Plot 8: change set global description...")
    x = chgsetsynthesis.loc[:,['nnode','nway','nrelation']]
    utils.multisctrplot(x, chgsetsynthesis.duration.astype('timedelta64[m]'),
                        lims=[0,np.log(max(x.apply(max))),0,700],
                        mkr="+", legend=True,
                        figpath="figs/chgset_modifvsduration.png",
                        xlab="Logarithm of modification quantity per change sets",
                        ylab="Change set duration (in minutes)")

    # Plot metadata: number of change sets per user
    print("Plot 9: number of change set per user...")
    utils.single_empCDF(usersynthesis.chgset.quantile(y), y, lims=[0,100,0,1],
                        xlab="Number of change sets per user",
                        ylab="Empirical CDF",
                        figpath="figs/nbchgsetperuser.png")
    
    # Plot metadata: number of modification per user
    print("Plot 10: number of modifications per user...")
    utils.single_empCDF(usersynthesis.elem.quantile(y), y,
                        lims=[0,100,0,1], hltqt=0.8,
                        xlab="Number of modifications per user",
                        ylab="Empirical CDF",
                        figpath="figs/nbmodifperuser.png")
    print("Plot 11: number of modifications per user and per type...")
    x = usersynthesis.loc[:,['nnode','nway','nrelation']].apply(
        lambda x: x.quantile(y))
    utils.multi_empCDF(x, y, lims=[0,200,0,1], hltqt=0.8, legend=True,
                       xlab="Number of modifications per user",
                       ylab="Empirical CDF",
                       figpath="figs/nbmodifperusertype.png")

    # Plot metadata: user global description
    print("Plot 12: user global description...")
    utils.scatterplot(usersynthesis.chgset,usersynthesis.elem, 
                      lims=[0,1000,0,10000], bsctr=True, mkr="s",
                      figpath="figs/user_chgsetvsmodif.png",
                      xlab="Number of change sets by user",
                      ylab="Number of modifications per user")
    print("Plot 13: number of contributors per element...")
    utils.scatterplot(usersynthesis.chgset,
                      usersynthesis.activity.astype('timedelta64[D]'),
                      lims=[0,100,0,3500], bsctr=True, mkr="+", 
                      figpath="figs/user_chgsetvsactivity.png",
                      xlab="Number of change sets by user",
                      ylab="User activity duration (in days)")
