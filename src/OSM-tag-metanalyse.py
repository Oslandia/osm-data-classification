# coding: utf-8

import sys
import os.path as osp

import pandas as pd

########################################

def tagvalue_analysis(genome, key, pivot_var='elem'):
    tag_analysis = (genome.query("tagkey==@key")
                    .groupby(['tagvalue',*pivot_var])['id']
                    .nunique()
                    .unstack()
                    .fillna(0))
    tag_analysis['total'] = tag_analysis.apply(sum, axis=1)
    return tag_analysis.sort_values('total', ascending=False)

########################################

if __name__=='__main__':

    if len(sys.argv) != 3:
        print("Usage: python <pathto_OSM-metadata-extract.py> <pathto_csvfiles> <dataset_name>")
        sys.exit(-1)
    datapath = sys.argv[1]
    dataset_name = sys.argv[2]
    tag_genome = pd.read_csv(datapath + "/" + dataset_name + "-tag-genome.csv",
                               index_col=0)

    tagcount = tag_genome.groupby('elem')['tagkey'].nunique().reset_index()

    tagkeycount = (tag_genome.groupby(['tagkey','elem'])['elem']
                   .count()
                   .unstack()
                   .fillna(0))
    tagkeycount['elem'] = tagkeycount.apply(sum, axis=1)
    tagkeycount = tagkeycount.sort_values('elem', ascending=False)
    tagkeycount.head(10)

    tag_analysis(tag_genome, 'highway', 'version')
