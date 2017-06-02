# coding: utf-8

""" Luigi implementation for OSM data analysis
"""

import os.path as osp

import luigi
from luigi.format import MixedUnicodeBytes, UTF8
import pandas as pd
import numpy as np

from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans

import osmparsing
import tagmetanalyse
import utils

class OSMHistoryParsing(luigi.Task):

    """ Luigi task : parse OSM data history from a .pbf file
    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")

    def outputpath(self):
        return osp.join(self.datarep, "output-extracts", self.dsname,
                        self.dsname+"-elements.csv")

    def output(self):
        return luigi.LocalTarget(self.outputpath())

    def run(self):
        tlhandler = osmparsing.TimelineHandler()
        datapath = osp.join(self.datarep, "raw", self.dsname+".osh.pbf")
        tlhandler.apply_file(datapath)
        colnames = ['elem', 'id', 'version', 'visible', 'ts',
                    'uid', 'chgset', 'ntags', 'tagkeys']
        elements = pd.DataFrame(tlhandler.elemtimeline, columns=colnames)
        elements = elements.sort_values(by=['elem', 'id', 'version'])
        with self.output().open('w') as outputflow:
            elements.to_csv(outputflow, date_format='%Y-%m-%d %H:%M:%S')


class OSMTagParsing(luigi.Task):

    """ Luigi task : parse OSM tag genome from a .pbf file
    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")

    def outputpath(self):
        return osp.join(self.datarep, "output-extracts", self.dsname,
                        self.dsname+"-tag-genome.csv")

    def output(self):
        return luigi.LocalTarget(self.outputpath())

    def run(self):
        taghandler = osmparsing.TagGenomeHandler()
        datapath = osp.join(self.datarep, "raw", self.dsname+".osh.pbf")
        taghandler.apply_file(datapath)
        print("There are {0} tag records in this dataset".format(len(taghandler.taggenome)))
        tag_genome = pd.DataFrame(taghandler.taggenome)
        tag_genome.columns = ['elem', 'id', 'version', 'tagkey', 'tagvalue']
        with self.output().open('w') as outputflow:
            tag_genome.to_csv(outputflow)


class OSMTagMetaAnalysis(luigi.Task):
    """ Luigi task: OSM tag genome meta analysis
    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")

    def outputpath(self):
        return osp.join(self.datarep, "output-extracts", self.dsname,
                        self.dsname+"-fulltag-analysis.csv")

    def output(self):
        return luigi.LocalTarget(self.outputpath())

    def requires(self):
        return {'history': OSMHistoryParsing(self.datarep, self.dsname),
                'taggenome': OSMTagParsing(self.datarep, self.dsname)}

    def run(self):
        with self.input()['history'].open('r') as inputflow:
            osm_elements = pd.read_csv(inputflow, index_col=0)
        with self.input()['taggenome'].open('r') as inputflow:
            tag_genome = pd.read_csv(inputflow, index_col=0)

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

        ### Analyse of tag key frequency (amongst all elements)
        fulltaganalys = pd.merge(osm_elements[['elem', 'id', 'version']],
                                 tag_genome,
                                 on=['elem','id','version'],
                                 how="outer")
        tagfreq = tagmetanalyse.tag_frequency(fulltaganalys,
                                              ['elem','version'])

        ### Analyse of tag value frequency (amongst all tagged elements)
        tagmetanalyse.tagvalue_analysis(tag_genome, 'highway', ['version'])

        ### Analyse of tag value frequency (amongst all tagged element)
        # For element with a specific tag key (e.g. 'highway')
        tagvalue_freq = tagmetanalyse.tagvalue_frequency(tag_genome, "highway",
                                                         ['elem','version'])

        with self.output().open('w') as outputflow:
            fulltaganalys.to_csv(outputflow, date_format='%Y-%m-%d %H:%M:%S')


class OSMElementEnrichment(luigi.Task):
    """ Luigi task: building of new features for OSM element history
    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")

    def outputpath(self):
        return osp.join(self.datarep, "output-extracts", self.dsname,
                        self.dsname+"-enriched-elements.csv")

    def output(self):
        return luigi.LocalTarget(self.outputpath())

    def requires(self):
        return OSMHistoryParsing(self.datarep, self.dsname)

    def run(self):
        with self.input().open('r') as inputflow:
            osm_elements = pd.read_csv(inputflow,
                                       index_col=0,
                                       parse_dates=['ts'])
        osm_elements.sort_values(by=['elem','id','version'])
        osm_elements = utils.enrich_osm_elements(osm_elements)
        with self.output().open('w') as outputflow:
            osm_elements.to_csv(outputflow, date_format='%Y-%m-%d %H:%M:%S')


class ElementMetadataExtract(luigi.Task):
    """ Luigi task: extraction of metadata for each OSM element
    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")

    def outputpath(self):
        return (self.datarep + "/output-extracts/" + self.dsname + "/" +
                self.dsname + "-elem-md.csv")

    def output(self):
        return luigi.LocalTarget(self.outputpath())

    def requires(self):
        return OSMElementEnrichment(self.datarep, self.dsname)

    def run(self):
        with self.input().open('r') as inputflow:
            osm_elements = pd.read_csv(inputflow,
                                       index_col=0,
                                       parse_dates=['ts'])
        elem_md = utils.extract_elem_metadata(osm_elements)
        with self.output().open('w') as outputflow:
            elem_md.to_csv(outputflow, date_format='%Y-%m-%d %H:%M:%S')

            
class ChangeSetMetadataExtract(luigi.Task):
    """ Luigi task: extraction of metadata for each OSM change set
    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")

    def outputpath(self):
        return osp.join(self.datarep, "output-extracts", self.dsname,
                        self.dsname+"-chgset-md.csv")

    def output(self):
        return luigi.LocalTarget(self.outputpath())

    def requires(self):
        return OSMElementEnrichment(self.datarep, self.dsname)

    def run(self):
        with self.input().open('r') as inputflow:
            osm_elements = pd.read_csv(inputflow,
                                       index_col=0,
                                       parse_dates=['ts'])
        chgset_md = utils.extract_chgset_metadata(osm_elements)
        with self.output().open('w') as outputflow:
            chgset_md.to_csv(outputflow, date_format='%Y-%m-%d %H:%M:%S')

            
class UserMetadataExtract(luigi.Task):
    """ Luigi task: extraction of metadata for each OSM user
    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")

    def outputpath(self):
        return osp.join(self.datarep, "output-extracts", self.dsname,
                        self.dsname+"-user-md.csv")

    def output(self):
        return luigi.LocalTarget(self.outputpath())

    def requires(self):
        return {'chgsets': ChangeSetMetadataExtract(self.datarep,
                                                    self.dsname),
                'enrichhist': OSMElementEnrichment(self.datarep,
                                                   self.dsname)}

    def run(self):
        with self.input()['chgsets'].open('r') as inputflow:
            chgset_md = pd.read_csv(inputflow, index_col=0)
        with self.input()['enrichhist'].open('r') as inputflow:
            osm_elements = pd.read_csv(inputflow,
                                       index_col=0,
                                       parse_dates=['ts'])
        user_md = utils.extract_user_metadata(osm_elements, chgset_md)
        with self.output().open('w') as outputflow:
            user_md.to_csv(outputflow, date_format='%Y-%m-%d %H:%M:%S')


class ChgsetPCA(luigi.Task):
    """ Luigi task: get change set PCA feature contributions
    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")
    nb_mindimensions = luigi.parameter.IntParameter(3)
    nb_maxdimensions = luigi.parameter.IntParameter(6)
    features = luigi.Parameter("elem")

    def outputpath(self):
        return osp.join(self.datarep, "output-extracts", self.dsname,
                        self.dsname+"-chgset-pca.h5")

    def output(self):
        return luigi.LocalTarget(self.outputpath(), format=MixedUnicodeBytes)

    def requires(self):
        return ChangeSetMetadataExtract(self.datarep, self.dsname)

    def set_nb_dimensions(self, var_analysis):
        candidate_npc = 0
        for i in range(len(var_analysis)):
            if var_analysis.iloc[i,0] < 1 or var_analysis.iloc[i,2] > 80:
                candidate_npc = i+1
                break
        if candidate_npc < self.nb_mindimensions:
            candidate_npc = self.nb_mindimensions
        if candidate_npc > self.nb_maxdimensions:
            candidate_npc = sellf.nb_maxdimensions
        return candidate_npc
    
    def run(self):
        with self.input().open('r') as inputflow:
            chgset_md  = pd.read_csv(inputflow,
                                       index_col=0,
                                       parse_dates=['first_at', 'last_at'])
        # Data preparation
        chgset_md = chgset_md.set_index(['chgset', 'uid'])
#        chgset_md = utils.extract_features(chgset_md, self.features)
        chgset_md = utils.drop_features(chgset_md, '_at')
        chgset_md = utils.drop_features(chgset_md, 'relation')
        chgset_md = utils.drop_features(chgset_md, 'way')
        chgset_md = utils.drop_features(chgset_md, 'node')
        print(chgset_md.columns)
        # Data normalization
        X = StandardScaler().fit_transform(chgset_md.values)
        # Select the most appropriate dimension quantity
        cov_mat = np.cov(X.T)
        eig_vals, eig_vecs = np.linalg.eig(cov_mat)
        eig_vals = sorted(eig_vals, reverse=True)
        tot = sum(eig_vals)
        var_exp = [(i/tot)*100 for i in eig_vals]
        cum_var_exp = np.cumsum(var_exp)
        var_analysis = pd.DataFrame({'eig': eig_vals,
                                     'varexp': var_exp,
                                     'cumvar': cum_var_exp})[['eig','varexp',
                                                              'cumvar']]
        print(var_analysis)
        # Run the PCA
        npc = self.set_nb_dimensions(var_analysis)
        print("npc = {0}".format(npc))
        pca = PCA(n_components=npc)
        Xpca = pca.fit_transform(X)
        pca_cols = ['PC' + str(i+1) for i in range(npc)]
        pca_var = pd.DataFrame(pca.components_, index=pca_cols,
                               columns=chgset_md.columns).T
        pca_ind = pd.DataFrame(Xpca, columns=pca_cols,
                               index=chgset_md.index.get_level_values('chgset'))
        # Save the PCA results into a binary file
        path = self.output().path
        pca_var.to_hdf(path, '/features')
        pca_ind.to_hdf(path, '/individuals')


class MasterTask(luigi.Task):
    """ Luigi task: generic task that launches every final tasks
    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")
    
    def requires(self):
        yield UserMetadataExtract(self.datarep, self.dsname)
        yield ElementMetadataExtract(self.datarep, self.dsname)
        yield OSMTagMetaAnalysis(self.datarep, self.dsname)
        yield ChgsetPCA(self.datarep, self.dsname, 3, 6, 'elem')
