# coding: utf-8

"""
OSM-tag-metanalysis.py: implement a short analysis of OSM element tag genome
How to call this module?
<python3 <pathto_module.py> <pathto_data> <dataset_name>
"""

import sys
import os.path as osp

import pandas as pd

########################################

def tagvalue_analysis(genome, key, pivot_var=['elem'], vrank=1):
    return (genome.query("tagkey==@key")
            .groupby(['tagvalue', *pivot_var])['id']
            .nunique()
            .unstack()
            .fillna(0))

def tagvalue_frequency(genome, key, pivot_var=['elem', 'version'], nround=2, vrank=1):
    total_uniqelem = (genome.query("tagkey==@key")
                      .groupby(pivot_var)['id']
                      .nunique()
                      .unstack()
                      .fillna(0))
    tagcount = tagvalue_analysis(genome, key, pivot_var)
    tagcount_groups = tagcount.groupby(level='elem')
    tag_freq = []
    for key, group in tagcount_groups:
        tag_freq.append( group / total_uniqelem.loc[key])
    tag_freq = pd.concat(tag_freq)
    return (100*tag_freq).round(nround)

def tagkey_analysis(genome, pivot_var=['elem']):
    return (genome.groupby(['tagkey', *pivot_var])['id']
            .nunique()
            .unstack()
            .fillna(0))

def tag_frequency(genome, pivot_var=['elem', 'version'], nround=2, vrank=1):
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

########################################

if __name__=='__main__':

    if len(sys.argv) != 3:
        print("Usage: python3 <pathto_module.py> <pathto_csvfiles> <dataset_name>")
        sys.exit(-1)
    datapath = sys.argv[1]
    dataset_name = sys.argv[2]
    tag_genome = pd.read_csv(datapath + "/" + dataset_name + "-tag-genome.csv",
                               index_col=0)
    osm_elements = pd.read_csv(datapath + "/" + dataset_name + "-elements.csv",
                               index_col=0)

    ### Tag count analysis
    # How many unique tag keys per OSM elements?
    tagcount = (tag_genome.groupby('elem')['tagkey']
                .nunique()
                .reset_index())

    # List of tag keys and number of elements they are associated with
    tagkeycount = (tag_genome.groupby(['tagkey','elem'])['elem']
                   .count()
                   .unstack()
                   .fillna(0))
    tagkeycount['elem'] = tagkeycount.apply(sum, axis=1)
    tagkeycount = tagkeycount.sort_values('elem', ascending=False)
    # The 10 most encountered tag keys in OSM history
    tagkeycount.head(10)

    ### Analyse of tag key frequency (amongst all elements)
    fulltaganalys = pd.merge(osm_elements[['elem', 'id', 'version']],
                                tag_genome,
                                on=['elem','id','version'],
                                how="outer")
    tagfreq = tag_frequency(fulltaganalys, ['elem','version'])
    # When are elements tagged with 'highway' keys?
    tagfreq.loc['highway']

    ### Analyse of tag value frequency (amongst all tagged elements)
    tagvalue_analysis(tag_genome, 'highway', ['version'])

    ### Analyse of tag value frequency (amongst all tagged element)
    # For element with a specific tag key (e.g. 'highway')
    tagvalue_freq = tagvalue_frequency(tag_genome, "highway", ['elem','version'])

    # How many elements are tagged with value 'residential', amongst "highway" elements?
    tagvalue_freq.loc['residential',0:10]
