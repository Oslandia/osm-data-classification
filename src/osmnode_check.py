# coding: utf-8

""" Test about the OSM node visibility: verify with http requests that unvalid coordinates refer to unvisible nodes """

import sys

import requests
import re
import pandas as pd

########################################
def elemvisibility(data, elemtype, samplesize=1000):
    elemsamp = data[['id','visible']].sample(samplesize)
    osmwebsite = "https://api.openstreetmap.org/api/0.6/" + elemtype
    elemsamp['url'] = [osmwebsite+"/%s" %elemid for elemid in elemsamp.id]
    elemsamp['hurl'] = ["%s/history" %elemurl for elemurl in elemsamp.url]
    elemsamp['status'] = elemsamp['url'].apply(lambda x: statusrequest(x))
    elemsamp['vsbltcheck'] = elemsamp['hurl'].apply(lambda x: vsbltrequest(x))
    return elemsamp

def statusrequest(address):
    return requests.get(address).status_code

def vsbltrequest(address):
    vsblt = re.findall('(visible="true"|visible="false")',
                       requests.get(address).text)[-1]
    return re.search('true', vsblt) is not None
    

########################################
if __name__ == '__main__':

    if len(sys.argv) != 3:
        print("Usage: python osmnode_check.py <data set name> <nbrequest>")
        sys.exit(-1)
    dataset_name = sys.argv[1]
    nbrequest = int(sys.argv[2])
    #save_output = True if sys.argv[2]=="y" or sys.argv[2]=="Y" else False
    datapath = "~/data/" + dataset_name + "/"

    ########################################
    osm_nodes = pd.read_csv(datapath + dataset_name + "-nodes.csv",
                            index_col=0, parse_dates=['ts'])
    nodevsblt = elemvisibility(osm_nodes, "node", nbrequest)

    print("Status-visibility frequency table for nodes:\n {0}"
          .format(pd.crosstab(index=nodevsblt["status"],
                              columns=nodevsblt["vsbltcheck"])))
    print("Visibility checking frequency table for nodes:\n {0}"
          .format(pd.crosstab(index=nodevsblt["visible"],
                              columns=nodevsblt["vsbltcheck"])))
    ########################################
    osm_ways = pd.read_csv(datapath + dataset_name + "-ways.csv",
                            index_col=0, parse_dates=['ts'])
    wayvsblt = elemvisibility(osm_ways, "way", nbrequest)
    
    print("Status-visibility frequency table for ways:\n {0}"
          .format(pd.crosstab(index=wayvsblt["status"],
                              columns=wayvsblt["vsbltcheck"])))
    print("Visibility checking frequency table for ways:\n {0}"
          .format(pd.crosstab(index=wayvsblt["visible"],
                              columns=wayvsblt["vsbltcheck"])))
    ########################################
    osm_relations = pd.read_csv(datapath + dataset_name + "-relations.csv",
                            index_col=0, parse_dates=['ts'])
    relvsblt = elemvisibility(osm_relations, "relation", nbrequest)
    
    print("Status-visibility frequency table for relations:\n {0}"
          .format(pd.crosstab(index=relvsblt["status"],
                              columns=relvsblt["vsbltcheck"])))
    print("Visibility checking frequency table for relations:\n {0}"
          .format(pd.crosstab(index=relvsblt["visible"],
                              columns=relvsblt["vsbltcheck"])))
