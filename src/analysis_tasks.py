# coding: utf-8

"""Module dedicated write some Luigi tasks for analysis purposes
"""

import os.path as osp
import random

import luigi
from luigi.format import MixedUnicodeBytes, UTF8

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

from sklearn.preprocessing import RobustScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

import data_preparation_tasks
from extract_user_editor import editor_count, get_top_editor, editor_name
import tagmetanalyse
import unsupervised_learning as ul
import utils

OUTPUT_DIR = 'output-extracts'

### OSM Evolution through time ####################################
class OSMChronology(luigi.Task):
    """ Luigi task: evaluation of OSM element historical evolution
    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")
    start_date = luigi.Parameter('2006-01-01')
    end_date = luigi.Parameter('2017-01-01')

    def outputpath(self):
        return osp.join(self.datarep, OUTPUT_DIR, self.dsname,
                        self.dsname+"-chronology.csv")

    def output(self):
        return luigi.LocalTarget(self.outputpath())

    def requires(self):
        return data_preparation_tasks.OSMHistoryParsing(self.datarep, self.dsname)

    def run(self):
        with self.input().open('r') as inputflow:
            osm_elements = pd.read_csv(inputflow,
                                       index_col=0,
                                       parse_dates=['ts'])
        osm_stats = utils.osm_chronology(osm_elements,
                                         self.start_date,
                                         self.end_date)
        with self.output().open('w') as outputflow:
            osm_stats.to_csv(outputflow, date_format='%Y-%m-%d %H:%M:%S')

### OSM tag genome analysis ####################################
class OSMTagCount(luigi.Task):
    """ Luigi task: OSM tag count
    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")

    def outputpath(self):
        return osp.join(self.datarep, OUTPUT_DIR, self.dsname,
                        self.dsname+"-tagcount.csv")

    def output(self):
        return luigi.LocalTarget(self.outputpath())

    def requires(self):
        return data_preparation_tasks.OSMTagParsing(self.datarep, self.dsname)

    def run(self):
        with self.input().open('r') as inputflow:
            tag_genome = pd.read_csv(inputflow, index_col=0)

        tagcount = (tag_genome.groupby('elem')['tagkey']
                    .nunique()
                    .reset_index())

        with self.output().open('w') as outputflow:
            tagcount.to_csv(outputflow, date_format='%Y-%m-%d %H:%M:%S')

class OSMTagKeyCount(luigi.Task):
    """ Luigi task: OSM tag key count
    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")

    def outputpath(self):
        return osp.join(self.datarep, OUTPUT_DIR, self.dsname,
                        self.dsname+"-tagkeycount.csv")

    def output(self):
        return luigi.LocalTarget(self.outputpath())

    def requires(self):
        return data_preparation_tasks.OSMTagParsing(self.datarep, self.dsname)

    def run(self):
        with self.input().open('r') as inputflow:
            tag_genome = pd.read_csv(inputflow, index_col=0)

        # List of tag keys and number of elements they are associated with
        tagkeycount = (tag_genome.groupby(['tagkey','elem'])['elem']
                       .count()
                       .unstack()
                       .fillna(0))
        tagkeycount['elem'] = tagkeycount.apply(sum, axis=1)
        tagkeycount = tagkeycount.sort_values('elem', ascending=False)

        with self.output().open('w') as outputflow:
            tagkeycount.to_csv(outputflow,
                               date_format='%Y-%m-%d %H:%M:%S')

class OSMTagFreq(luigi.Task):
    """ Luigi task: analyse of tag key frequency (amongst all elements)
    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")

    def outputpath(self):
        return osp.join(self.datarep, OUTPUT_DIR, self.dsname,
                        self.dsname+"-tagfreq.csv")

    def output(self):
        return luigi.LocalTarget(self.outputpath())

    def requires(self):
        return {'history': data_preparation_tasks.OSMHistoryParsing(self.datarep,
                                                                  self.dsname),
                'taggenome': data_preparation_tasks.OSMTagParsing(self.datarep,
                                                                self.dsname)}

    def run(self):
        with self.input()['history'].open('r') as inputflow:
            osm_elements = pd.read_csv(inputflow, index_col=0)
        with self.input()['taggenome'].open('r') as inputflow:
            tag_genome = pd.read_csv(inputflow, index_col=0)
        fulltaganalys = pd.merge(osm_elements[['elem', 'id', 'version']],
                                 tag_genome,
                                 on=['elem','id','version'],
                                 how="outer")
        tagfreq = tagmetanalyse.tag_frequency(fulltaganalys,
                                              ['elem','version'])
        with self.output().open('w') as outputflow:
            tagfreq.to_csv(outputflow, date_format='%Y-%m-%d %H:%M:%S')

class OSMTagValue(luigi.Task):
    """ Luigi task: analyse of tag value frequency (among all tagged elements)
    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")

    def outputpath(self):
        return osp.join(self.datarep, OUTPUT_DIR, self.dsname,
                        self.dsname+"-tagvalue.csv")

    def output(self):
        return luigi.LocalTarget(self.outputpath())

    def requires(self):
        return data_preparation_tasks.OSMTagParsing(self.datarep, self.dsname)

    def run(self):
        with self.input().open('r') as inputflow:
            tag_genome = pd.read_csv(inputflow, index_col=0)
        tagvalue = tagmetanalyse.tagvalue_analysis(tag_genome, 'highway',
                                                   ['version'])
        with self.output().open('w') as outputflow:
            tagvalue.to_csv(outputflow, date_format='%Y-%m-%d %H:%M:%S')

class OSMTagValueFreq(luigi.Task):
    """Luigi task: Analyse of tag value frequency 
    (amongst all tagged element), with a specific tag key (e.g. 'highway')
    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")

    def outputpath(self):
        return osp.join(self.datarep, OUTPUT_DIR, self.dsname,
                        self.dsname+"-tagvalue-freq.csv")

    def output(self):
        return luigi.LocalTarget(self.outputpath())

    def requires(self):
        return data_preparation_tasks.OSMTagParsing(self.datarep, self.dsname)

    def run(self):
        with self.input().open('r') as inputflow:
            tag_genome = pd.read_csv(inputflow, index_col=0)
        tagvalue_freq = tagmetanalyse.tagvalue_frequency(tag_genome,
                                                         "highway",
                                                         ['elem', 'version'])
        with self.output().open('w') as outputflow:
            tagvalue_freq.to_csv(outputflow, date_format='%Y-%m-%d %H:%M:%S')

            
### OSM Metadata Extraction ####################################
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
        return data_preparation_tasks.OSMElementEnrichment(self.datarep,
                                                         self.dsname)

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
        return osp.join(self.datarep, OUTPUT_DIR, self.dsname,
                        self.dsname+"-chgset-md.csv")

    def output(self):
        return luigi.LocalTarget(self.outputpath())

    def requires(self):
        return data_preparation_tasks.OSMElementEnrichment(self.datarep, self.dsname)

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
        return osp.join(self.datarep, OUTPUT_DIR, self.dsname,
                        self.dsname+"-user-md.csv")

    def output(self):
        return luigi.LocalTarget(self.outputpath())

    def requires(self):
        return {'chgsets': ChangeSetMetadataExtract(self.datarep, self.dsname),
                'enrichhist': data_preparation_tasks.OSMElementEnrichment(self.datarep, self.dsname),
                'chgset_kmeans': MetadataKmeans(self.datarep, self.dsname,
                                                "chgset", "auto", 3, 10)}

    def run(self):
        with self.input()['chgsets'].open('r') as inputflow:
            chgset_md = pd.read_csv(inputflow, index_col=0)
        with self.input()['enrichhist'].open('r') as inputflow:
            osm_elements = pd.read_csv(inputflow,
                                       index_col=0,
                                       parse_dates=['ts'])
        inputpath = self.input()['chgset_kmeans'].path
        chgset_kmeans = pd.read_hdf(inputpath, 'individuals')
        chgset_md = pd.merge(chgset_md,
                             chgset_kmeans.reset_index()[['chgset', 'Xclust']],
                             on='chgset')
        user_md = utils.extract_user_metadata(osm_elements, chgset_md)
        with self.output().open('w') as outputflow:
            user_md.to_csv(outputflow, date_format='%Y-%m-%d %H:%M:%S')

### OSM Editor analysis ####################################
class TopMostUsedEditors(luigi.Task):
    """Compute the most used editor. Transform the editor name such as JOSM/1.2.3
    into josm in order to have
    """
    datarep = luigi.Parameter("data")
    fname = 'most-used-editor'
    editor_fname = 'all-editors-by-user.csv'

    def output(self):
        return luigi.LocalTarget(
            osp.join(self.datarep, OUTPUT_DIR, self.fname + ".csv"),
            format=UTF8)

    def run(self):
        with open(osp.join(self.datarep, OUTPUT_DIR, self.editor_fname)) as fobj:
            user_editor = pd.read_csv(fobj, header=None, names=['uid', 'value', 'num'])
        # extract the unique editor name aka fullname
        user_editor['fullname'] = user_editor['value'].apply(editor_name)
        editor = editor_count(user_editor)
        top_editor = get_top_editor(editor)
        with self.output().open('w') as fobj:
            top_editor.to_csv(fobj, index=False)

class EditorCountByUser(luigi.Task):
    datarep = luigi.Parameter("data")
    # take first 9th most used editors (the tenth being "others")
    n_top_editor = luigi.IntParameter(default=5)
    editor_fname = 'all-editors-by-user.csv'
    fname = 'editors-count-by-user.csv'

    def output(self):
        return luigi.LocalTarget(osp.join(self.datarep, OUTPUT_DIR, self.fname),
                                 format=UTF8)

    def requires(self):
        return TopMostUsedEditors(self.datarep)

    def run(self):
        with open(osp.join(self.datarep, OUTPUT_DIR, self.editor_fname)) as fobj:
            user_editor = pd.read_csv(fobj, header=None,
                                      names=['uid', 'value', 'num'])
        # extract the unique editor name aka fullname
        user_editor['fullname'] = user_editor['value'].apply(editor_name)
        with self.input().open('r') as fobj:
            top_editor = pd.read_csv(fobj)
        selection = (top_editor.fullname[:self.n_top_editor].tolist()
                     + ['other'])
        # Set the 'other' label for editors which are not in the top selection
        other_mask = np.logical_not(user_editor['fullname'].isin(selection))
        user_editor.loc[other_mask, 'fullname'] = 'other'
        data = (user_editor.groupby(["uid", "fullname"])["num"].sum()
                .unstack()
                .reset_index()
                .fillna(0))
        data.columns.values[1:] = ['n_total_chgset_'+name.replace(' ', '_')
                            for name in data.columns.values[1:]]
        with self.output().open("w") as fobj:
            data.to_csv(fobj, index=False)

class AddExtraInfoUserMetadata(luigi.Task):
    """Add extra info to User metadata such as used editor and total number of
    changesets
    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")
    n_top_editor = luigi.IntParameter(default=5)
    editor_fname = 'editor-counts-by-user.csv'
    total_user_changeset_fname = 'all-changesets-by-user.csv'

    def output(self):
        return luigi.LocalTarget(
            osp.join(self.datarep, OUTPUT_DIR, self.dsname,
                     self.dsname + "-user-md-extra.csv"), format=UTF8)

    def requires(self):
        return {'editor_count_by_user': EditorCountByUser(self.datarep, self.n_top_editor),
                'user_metadata': UserMetadataExtract(self.datarep, self.dsname)}

    def run(self):
        with self.input()['user_metadata'].open() as fobj:
            users = pd.read_csv(fobj, index_col=0)
        with self.input()['editor_count_by_user'].open() as fobj:
            user_editor = pd.read_csv(fobj)
        with open(osp.join(self.datarep, OUTPUT_DIR, self.total_user_changeset_fname)) as fobj:
            changeset_count_users = pd.read_csv(fobj, header=None,
                                                names=['uid', 'num'])
        users = utils.add_chgset_metadata(users, changeset_count_users)
        users = utils.add_editor_metadata(users, user_editor)
        # users = utils.transform_editor_features(users)
        with self.output().open('w') as fobj:
            users.to_csv(fobj)


### OSM Metadata analysis with unsupervised learning tool #########
class MetadataNormalization(luigi.Task):
    """ Luigi task: normalize every features into metadata, so as to apply PCA
    and Kmeans
    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")
    
    def outputpath(self):
        return osp.join(self.datarep, OUTPUT_DIR, self.dsname,
                        self.dsname+"-user-md-norm.csv")

    def output(self):
        return luigi.LocalTarget(self.outputpath())
    
    def requires(self):
        return {'osmelem':
        data_preparation_tasks.OSMElementEnrichment(self.datarep, self.dsname),
                'metadata': AddExtraInfoUserMetadata(self.datarep, self.dsname)}

    def run(self):
        with self.input()['osmelem'].open('r') as inputflow:
            osm_elements  = pd.read_csv(inputflow, index_col=0,
                                        parse_dates=['ts'])
        with self.input()['metadata'].open('r') as inputflow:
            metadata  = pd.read_csv(inputflow, index_col=0)
        timehorizon = osm_elements.ts.max() - osm_elements.ts.min()
        utils.normalize_temporal_features(metadata, timehorizon)
        utils.normalize_features(metadata, 'n_node_modif')
        utils.normalize_features(metadata, 'n_way_modif')
        utils.normalize_features(metadata, 'n_relation_modif')
        metadata = utils.ecdf_transform(metadata, 'nmean_modif_byelem')
        metadata = utils.ecdf_transform(metadata, 'n_node_modif')
        metadata = utils.ecdf_transform(metadata, 'n_way_modif')
        metadata = utils.ecdf_transform(metadata, 'n_relation_modif')
        metadata = utils.transform_editor_features(metadata)
        with self.output().open('w') as fobj:
            metadata.to_csv(fobj)
        

class MetadataPCA(luigi.Task):
    """ Luigi task: compute PCA for any metadata
    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")
    metadata_type = luigi.Parameter("user")
    select_param_mode = luigi.Parameter("auto")
    nb_mindimensions = luigi.parameter.IntParameter(3)
    nb_maxdimensions = luigi.parameter.IntParameter(12)
    features = luigi.Parameter('')

    def outputpath(self):
        return osp.join(self.datarep, OUTPUT_DIR, self.dsname,
                        self.dsname+"-"+self.metadata_type+"-pca.h5")

    def output(self):
        return luigi.LocalTarget(self.outputpath(), format=MixedUnicodeBytes)

    def requires(self):
        if self.metadata_type == "chgset":
            return ChangeSetMetadataExtract(self.datarep, self.dsname)
        elif self.metadata_type == "user":
            return MetadataNormalization(self.datarep, self.dsname)
        else:
            raise ValueError("Metadata type '{}' not known. Please use 'user' or 'chgset'".format(self.metadata_type))
        
    def compute_nb_dimensions(self, var_analysis):
        """Return a number of components that is supposed to be optimal,
        regarding the variance matrix (low eigenvalues, sufficient explained
        variance threshold); if
        self.select_param_mode=="manual", the user must enter its preferred
        number of components based on variance-related barplots
        
        """
        if self.select_param_mode == "manual":
            ul.plot_pca_variance(var_analysis)
            plt.show()
            nb_components = input("# \n# Enter the number of components: \n# ")
            return int(nb_components)
        candidate_npc = 0
        for i in range(len(var_analysis)):
            if var_analysis.iloc[i,0] < 1 or var_analysis.iloc[i,2] > 80:
                candidate_npc = i+1
                break
        if candidate_npc < self.nb_mindimensions:
            candidate_npc = self.nb_mindimensions
        if candidate_npc > self.nb_maxdimensions:
            candidate_npc = self.nb_maxdimensions
        return candidate_npc

    def run(self):
        with self.input().open('r') as inputflow:
            metadata  = pd.read_csv(inputflow,
                                       index_col=0)
        # Data preparation
        if self.metadata_type == "chgset":
            metadata = metadata.set_index(['chgset', 'uid'])
        metadata = utils.drop_features(metadata, '_at')
        if self.features != '':
            for pattern in ['elem', 'node', 'way', 'relation']:
                if pattern != self.features:
                    metadata = utils.drop_features(metadata, pattern)
        # Data normalization
        scaler = RobustScaler(quantile_range=(0.0,100.0)) # = Min scaler
        X = scaler.fit_transform(metadata.values)
        # Select the most appropriate dimension quantity
        var_analysis = ul.compute_pca_variance(X)
        # Run the PCA
        npc = self.compute_nb_dimensions(var_analysis)
        pca = PCA(n_components=npc)
        Xpca = pca.fit_transform(X)
        pca_cols = ['PC' + str(i+1) for i in range(npc)]
        pca_var = pd.DataFrame(pca.components_, index=pca_cols,
                               columns=metadata.columns).T
        if self.metadata_type == "chgset":
            pca_ind = pd.DataFrame(Xpca, columns=pca_cols,
                                   index=(metadata.index
                                          .get_level_values('chgset')))
        else:
            pca_ind = pd.DataFrame(Xpca, columns=pca_cols, index=metadata.index)
        # Save the PCA results into a binary file
        path = self.output().path
        pca_var.to_hdf(path, '/features')
        pca_ind.to_hdf(path, '/individuals')
        var_analysis.to_hdf(path, '/variance')

class MetadataKmeans(luigi.Task):
    """Luigi task: classify any metadata with a kmeans algorithm; a PCA procedure
    is a prerequisite for this task
    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")
    metadata_type = luigi.Parameter("user")
    select_param_mode = luigi.Parameter("auto")
    use_elbow = luigi.parameter.BoolParameter(True)
    use_silhouette = luigi.parameter.BoolParameter(True)
    nbmin_clusters = luigi.parameter.IntParameter(3)
    nbmax_clusters = luigi.parameter.IntParameter(8)
    
    def outputpath(self):
        return osp.join(self.datarep, OUTPUT_DIR, self.dsname,
                        self.dsname+"-"+self.metadata_type+"-kmeans.h5")

    def output(self):
        return luigi.LocalTarget(self.outputpath())

    def requires(self):
        return MetadataPCA(self.datarep, self.dsname,
                           self.metadata_type, self.select_param_mode)

    def compute_nb_clusters(self, Xpca):
        """Compute kmeans for each cluster number until nbmax_clusters+1 to find the
        optimal number of clusters; if self.select_param_mode=="manual", the
        user must enter its preferred number of clusters based on the elbow
        plot: the visual selection criteria are elbow and/or silhouette methods,
        according to Luigi parameters

        """
        if not self.use_elbow and not self.use_silhouette:
            self.select_param_mode = "auto"
        if self.use_elbow:
            scores = []
        if self.use_silhouette:
            silhouette = []
        for i in range(1, self.nbmax_clusters + 1):
            kmeans = KMeans(n_clusters=i, n_init=100, max_iter=1000)
            kmeans.fit(Xpca)
            if self.use_elbow:
                scores.append(kmeans.inertia_)
            if self.use_silhouette:
                Xclust = kmeans.fit_predict(Xpca)
                silhouette_avg = []
                if i == 1:
                    silhouette.append(np.repeat(1,1000))
                    continue
                for k in range(10):
                    s = random.sample(range(len(Xpca)), 2000)
                    Xsampled = Xpca[s]
                    Csampled = Xclust[s]
                    while(len(np.unique(Csampled))==1):
                        s = random.sample(range(len(Xpca)), 2000)
                        Xsampled = Xpca[s]
                        Csampled = Xclust[s]
                    silhouette_avg.append(silhouette_score(X=Xsampled,
                                                           labels=Csampled))
                silhouette.append(silhouette_avg)
        if self.select_param_mode == "manual":
            ul.plot_cluster_decision(range(1, self.nbmax_clusters+1),
                                     scores,
                                     silhouette)
            nb_clusters = input("# \n# Enter the number of clusters: \n# ")
            return int(nb_clusters)
        elbow_deriv = ul.elbow_derivation(scores, self.nbmin_clusters)
        nbc =  1 + elbow_deriv.index(max(elbow_deriv))
        return nbc
    
    def run(self):
        inputpath = self.input().path
        pca_ind  = pd.read_hdf(inputpath, 'individuals')
        kmeans = KMeans(n_clusters=self.compute_nb_clusters(pca_ind.values),
                        n_init=100, max_iter=1000)
        kmeans_ind = pca_ind.copy()
        kmeans_ind['Xclust'] = kmeans.fit_predict(pca_ind.values)
        kmeans_centroids = pd.DataFrame(kmeans.cluster_centers_,
                                        columns=pca_ind.columns)
        kmeans_centroids['n_individuals'] = (kmeans_ind
                                             .groupby('Xclust')
                                             .count())['PC1']
        # Save the kmeans results into a binary file
        path = self.output().path
        kmeans_ind.to_hdf(path, '/individuals')
        kmeans_centroids.to_hdf(path, '/centroids')        
    
