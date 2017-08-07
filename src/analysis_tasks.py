# coding: utf-8

"""Module dedicated write some Luigi tasks for analysis purposes
"""

import os.path as osp

import luigi
from luigi.format import MixedUnicodeBytes, UTF8

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
        return osp.join(self.datarep, OUTPUT_DIR, self.dsname, "chronology.csv")

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
        return osp.join(self.datarep, OUTPUT_DIR, self.dsname, "tagcount.csv")

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
        return osp.join(self.datarep, OUTPUT_DIR, self.dsname, "tagkey-count.csv")

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
        return osp.join(self.datarep, OUTPUT_DIR, self.dsname, "tag-freq.csv")

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
        return osp.join(self.datarep, OUTPUT_DIR, self.dsname, "tag-value.csv")

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
        return osp.join(self.datarep, OUTPUT_DIR, self.dsname, "tag-value-freq.csv")

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
class ChangeSetMetadataExtract(luigi.Task):
    """ Luigi task: extraction of metadata for each OSM change set
    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")

    def outputpath(self):
        return osp.join(self.datarep, OUTPUT_DIR, self.dsname, "changeset-metadata.csv")

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
        return osp.join(self.datarep, OUTPUT_DIR, self.dsname, "user-metadata.csv")

    def output(self):
        return luigi.LocalTarget(self.outputpath())

    def requires(self):
        return {'changeset': ChangeSetMetadataExtract(self.datarep, self.dsname),
                'enrichhist': data_preparation_tasks.OSMElementEnrichment(self.datarep, self.dsname),
                'changeset_kmeans': KMeansTask(self.datarep, self.dsname, "changeset", 6)}

    def run(self):
        with self.input()['changeset'].open('r') as inputflow:
            chgset_md = pd.read_csv(inputflow, index_col=0)
        with self.input()['enrichhist'].open('r') as inputflow:
            osm_elements = pd.read_csv(inputflow,
                                       index_col=0,
                                       parse_dates=['ts'])
        inputpath = self.input()['changeset_kmeans'].path
        chgset_kmeans = pd.read_hdf(inputpath, 'individuals')
        chgset_md = pd.merge(chgset_md,
                             chgset_kmeans.reset_index()[['chgset', 'Xclust']],
                             on='chgset')
        user_md = utils.extract_user_metadata(osm_elements, chgset_md)
        with self.output().open('w') as outputflow:
            user_md.to_csv(outputflow, date_format='%Y-%m-%d %H:%M:%S')

class ElementMetadataExtract(luigi.Task):
    """ Luigi task: extraction of metadata for each OSM element
    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")

    def outputpath(self):
        return osp.join(self.datarep, OUTPUT_DIR, self.dsname, "element-metadata.csv")

    def output(self):
        return luigi.LocalTarget(self.outputpath())

    def requires(self):
        return {
            'osm_elements': data_preparation_tasks.OSMElementEnrichment(self.datarep, self.dsname),
            'user_groups': MetadataKmeans(self.datarep, self.dsname, 'user', 'manual')}

    def run(self):
        with self.input()['osm_elements'].open('r') as inputflow:
            osm_elements = pd.read_csv(inputflow,
                                       index_col=0,
                                       parse_dates=['ts'])
        inputpath = self.input()['user_groups'].path
        user_kmind  = pd.read_hdf(inputpath, 'individuals')
        elem_md = utils.extract_elem_metadata(osm_elements, user_kmind,
                                              drop_ts=False)
        with self.output().open('w') as outputflow:
            elem_md.to_csv(outputflow, date_format='%Y-%m-%d %H:%M:%S')


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
    # take first 5th most used editors
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
        data['known'] = data.iloc[:,1:].apply(lambda x: x.sum(), axis=1)
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
            osp.join(self.datarep, OUTPUT_DIR, self.dsname, "user-metadata-extra.csv"), format=UTF8)

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
        with self.output().open('w') as fobj:
            users.to_csv(fobj)


### OSM Metadata analysis with unsupervised learning tool #########
class MetadataNormalization(luigi.Task):
    """ Luigi task: normalize every features into metadata, so as to apply PCA
    and Kmeans
    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")
    metadata_type = luigi.Parameter("user")

    def outputpath(self):
        return osp.join(self.datarep, OUTPUT_DIR, self.dsname, self.metadata_type + "-metadata-norm.csv")

    def output(self):
        return luigi.LocalTarget(self.outputpath())

    def requires(self):
        if self.metadata_type == "changeset":
            return {'osmelem': data_preparation_tasks.OSMElementEnrichment(self.datarep, self.dsname),
                    'metadata': ChangeSetMetadataExtract(self.datarep, self.dsname)}
        elif self.metadata_type == "user":
            return {'osmelem': data_preparation_tasks.OSMElementEnrichment(self.datarep, self.dsname),
                    'metadata': AddExtraInfoUserMetadata(self.datarep, self.dsname)}
        else:
            raise ValueError("Metadata type '{}' not known. Please use 'user' or 'changeset'".format(self.metadata_type))

    def run(self):
        with self.input()['osmelem'].open('r') as inputflow:
            osm_elements  = pd.read_csv(inputflow, index_col=0,
                                        parse_dates=['ts'])
        with self.input()['metadata'].open('r') as inputflow:
            metadata  = pd.read_csv(inputflow, index_col=0)
        timehorizon = ((osm_elements.ts.max() - osm_elements.ts.min())
                       / pd.Timedelta('1d'))
        if self.metadata_type == "changeset":
            # By definition, a change set takes 24h max
            utils.normalize_temporal_features(metadata,
                                              24*60, timehorizon)
        else:
            utils.normalize_temporal_features(metadata,
                                              timehorizon, timehorizon)
        if self.metadata_type == "changeset":
            self.metadata_type # TODO - chgset normalization
        else:
            utils.normalize_features(metadata, 'n_total_modif')
            utils.normalize_features(metadata, 'n_node_modif')
            utils.normalize_features(metadata, 'n_way_modif')
            utils.normalize_features(metadata, 'n_relation_modif')
            metadata = utils.ecdf_transform(metadata, 'nmean_modif_byelem')
            metadata = utils.ecdf_transform(metadata, 'n_total_modif')
            metadata = utils.ecdf_transform(metadata, 'n_node_modif')
            metadata = utils.ecdf_transform(metadata, 'n_way_modif')
            metadata = utils.ecdf_transform(metadata, 'n_relation_modif')
            metadata = utils.transform_editor_features(metadata)
        with self.output().open('w') as fobj:
            metadata.to_csv(fobj)

class SinglePCA(luigi.Task):
    """Compute a PCA for a type of metadata according to a number of components (6 by default).
    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")
    # can be 'chgset' for changesets
    # XXX Raise an error if it's neither 'user' or 'chgset'
    metadata_type = luigi.Parameter("user")
    n_components = luigi.IntParameter(default=6)
    # Keep only this feature
    # XXX Make it more modular/robust
    features = luigi.Parameter('')

    def outputpath(self):
        fname = "-".join([self.metadata_type, "metadata",
                          "n_components", str(self.n_components),
                          "pca.h5"])
        return osp.join(self.datarep, OUTPUT_DIR, self.dsname, fname)

    def output(self):
        return luigi.LocalTarget(self.outputpath(), format=MixedUnicodeBytes)

    def requires(self):
        return MetadataNormalization(self.datarep, self.dsname,
                                     self.metadata_type)

    def run(self):
        with self.input().open('r') as inputflow:
            metadata  = pd.read_csv(inputflow, index_col=0)
        # Data preparation
        if self.metadata_type == "chgset":
            metadata = metadata.set_index(['chgset', 'uid'])
        metadata = utils.drop_features(metadata, '_at')
        if self.features != '':
            for pattern in ['elem', 'node', 'way', 'relation']:
                if pattern != self.features:
                    metadata = utils.drop_features(metadata, pattern)
        # Data normalization
        scaler = RobustScaler(quantile_range=(0.0, 100.0)) # = Min scaler
        X = scaler.fit_transform(metadata.values)
        pca = PCA(n_components=self.n_components)
        Xpca = pca.fit_transform(X)
        pca_cols = ['PC' + str(i+1) for i in range(self.n_components)]
        pca_var = pd.DataFrame(pca.components_, index=pca_cols,
                               columns=metadata.columns).T
        if self.metadata_type == "chgset":
            pca_ind = pd.DataFrame(Xpca, columns=pca_cols,
                                   index=(metadata.index
                                          .get_level_values('chgset')))
        else:
            pca_ind = pd.DataFrame(Xpca, columns=pca_cols, index=metadata.index)
        # Save the PCA results into a hdf5 (binary) file
        path = self.output().path
        pca_var.to_hdf(path, '/features')
        pca_ind.to_hdf(path, '/individuals')


class VarianceAnalysisTask(luigi.Task):
    """Dedicated to analyze the variance of some metadata
    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")
    metadata_type = luigi.Parameter("user")
    nb_mindimensions = luigi.parameter.IntParameter(3)
    nb_maxdimensions = luigi.parameter.IntParameter(12)
    features = luigi.Parameter('')

    def outputpath(self):
        fname = "-".join([self.metadata_type, "metadata",
                          "variance-analysis",
                          "min", str(self.nb_mindimensions),
                          "max", str(self.nb_maxdimensions) + ".csv"])
        return osp.join(self.datarep, OUTPUT_DIR, self.dsname, fname)

    def output(self):
        return luigi.LocalTarget(self.outputpath(), format=UTF8)

    def requires(self):
        return MetadataNormalization(self.datarep, self.dsname,
                                     self.metadata_type)

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
        with self.output().open("w") as fobj:
            var_analysis.to_csv(fobj, index=False)

class PlottingVarianceAnalysis(luigi.Task):
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")
    metadata_type = luigi.Parameter("user")
    nb_min_dim = luigi.parameter.IntParameter(3)
    nb_max_dim = luigi.parameter.IntParameter(12)
    features = luigi.Parameter('')

    def outputpath(self):
        fname = "-".join([self.metadata_type, "metadata",
                          "variance-analysis",
                          "min", str(self.nb_min_dim),
                          "max", str(self.nb_max_dim) + ".png"])
        return osp.join(self.datarep, OUTPUT_DIR, self.dsname, fname)

    def output(self):
        return luigi.LocalTarget(self.outputpath(), format=MixedUnicodeBytes)

    def requires(self):
        return VarianceAnalysisTask(self.datarep, self.dsname,
                                    self.metadata_type, self.nb_min_dim,
                                    self.nb_max_dim, self.features)

    def run(self):
        with self.input().open() as fobj:
            variance = pd.read_csv(fobj)
        fig = ul.plot_pca_variance(variance)
        fig.savefig(self.output().path)


class AutoPCA(luigi.Task):
    """Compute the optimal number of components for the PCA before carrying out the
    PCA for the user or changeset metadata
    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")
    metadata_type = luigi.Parameter("user")
    nb_min_dim = luigi.parameter.IntParameter(3)
    nb_max_dim = luigi.parameter.IntParameter(12)
    features = luigi.Parameter('')

    def outputpath(self):
        fname = "-".join([self.metadata_type, "metadata",
                          "auto-n_components",
                          "min", str(self.nb_min_dim),
                          "max", str(self.nb_max_dim),
                          "pca.h5"])
        return osp.join(self.datarep, OUTPUT_DIR, self.dsname, fname)

    def output(self):
        return luigi.LocalTarget(self.outputpath(), format=MixedUnicodeBytes)

    def requires(self):
        return {'variance': VarianceAnalysisTask(self.datarep, self.dsname,
                                                 self.metadata_type, self.nb_min_dim,
                                                 self.nb_max_dim, self.features),
                "metadata": MetadataNormalization(self.datarep, self.dsname,
                                                  self.metadata_type)}

    def run(self):
        with self.input()['metadata'].open('r') as inputflow:
            metadata  = pd.read_csv(inputflow, index_col=0)
        with self.input()['variance'].open() as fobj:
            variance = pd.read_csv(fobj)
        n_components = ul.optimal_PCA_components(variance, self.nb_min_dim,
                                                 self.nb_max_dim)
        # Data preparation
        if self.metadata_type == "changeset":
            metadata = metadata.set_index(['chgset', 'uid'])
        metadata = utils.drop_features(metadata, '_at')
        if self.features != '':
            for pattern in ['elem', 'node', 'way', 'relation']:
                if pattern != self.features:
                    metadata = utils.drop_features(metadata, pattern)
        # Data normalization
        scaler = RobustScaler(quantile_range=(0.0, 100.0)) # = Min scaler
        X = scaler.fit_transform(metadata.values)
        pca = PCA(n_components=n_components)
        Xpca = pca.fit_transform(X)
        pca_cols = ['PC' + str(i + 1) for i in range(n_components)]
        pca_var = pd.DataFrame(pca.components_, index=pca_cols,
                               columns=metadata.columns).T
        if self.metadata_type == "changeset":
            pca_ind = pd.DataFrame(Xpca, columns=pca_cols,
                                   index=(metadata.index
                                          .get_level_values('chgset')))
        else:
            pca_ind = pd.DataFrame(Xpca, columns=pca_cols, index=metadata.index)
        # Save the PCA results into a hdf5 (binary) file
        path = self.output().path
        pca_var.to_hdf(path, '/features')
        pca_ind.to_hdf(path, '/individuals')


class KMeansFromPCA(luigi.Task):
    """Simple KMeans according to some metadata: user or changeset
    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")
    metadata_type = luigi.Parameter("user")
    # for the underlying PCA. Put 0 if you don't want to choose a number of
    # components (an optimal way does it for you!)
    n_components = luigi.IntParameter(default=6)
    nb_clusters = luigi.IntParameter(default=5)

    def outputpath(self):
        if self.n_components == 0:
            n_components = 'auto'
        else:
            n_components = str(self.n_components)
        fname = "-".join([self.metadata_type, "metadata",
                          "pca", n_components,
                          "clusters", str(self.nb_clusters),
                          "kmeans.h5"])
        return osp.join(self.datarep, OUTPUT_DIR, self.dsname, fname)

    def output(self):
        return luigi.LocalTarget(self.outputpath())

    def requires(self):
        if self.n_components == 0:
            return AutoPCA(self.datarep, self.dsname, self.metadata_type)
        return SinglePCA(self.datarep, self.dsname, self.metadata_type, self.n_components)

    def run(self):
        inputpath = self.input().path
        pca_ind  = pd.read_hdf(inputpath, 'individuals')
        kmeans = KMeans(n_clusters=self.nb_clusters,
                        n_init=100, max_iter=1000)
        kmeans_ind = pca_ind.copy()
        kmeans_ind['Xclust'] = kmeans.fit_predict(pca_ind.values)
        kmeans_centroids = pd.DataFrame(kmeans.cluster_centers_,
                                        columns=pca_ind.columns)
        kmeans_centroids['n_individuals'] = (kmeans_ind
                                             .groupby('Xclust')
                                             .count())['PC1']
        # Save the kmeans results into a hdf5 file
        path = self.output().path
        kmeans_ind.to_hdf(path, '/individuals')
        kmeans_centroids.to_hdf(path, '/centroids')

class KMeansFromRaw(luigi.Task):
    """Simple KMeans according to some metadata: user or changeset. Take normalized
    raw features instead of the results of a PCA.
    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")
    metadata_type = luigi.Parameter("user")
    scaled = luigi.BoolParameter(default=True)
    nb_clusters = luigi.IntParameter(default=5)

    def outputpath(self):
        if self.scaled:
            label = 'scaled'
        else:
            label = 'raw'
        fname = "-".join([self.metadata_type, "metadata",
                          label, "data", "clusters",
                          str(self.nb_clusters), "kmeans.h5"])
        return osp.join(self.datarep, OUTPUT_DIR, self.dsname, fname)

    def output(self):
        return luigi.LocalTarget(self.outputpath())

    def requires(self):
        return MetadataNormalization(self.datarep, self.dsname,
                                     self.metadata_type)
    def run(self):
        with self.input().open('r') as inputflow:
            features  = pd.read_csv(inputflow, index_col=0)
        kmeans = KMeans(n_clusters=self.nb_clusters,
                        n_init=100, max_iter=1000)
        # each individual
        kmeans_ind = features.copy()
        # Data normalization
        if self.scaled:
            scaler = RobustScaler(quantile_range=(0.0, 100.0)) # = Min scaler
            X = scaler.fit_transform(features.values)
            kmeans_ind['Xclust'] = kmeans.fit_predict(X)
        else:
            kmeans_ind['Xclust'] = kmeans.fit_predict(features.values)
        kmeans_centroids = pd.DataFrame(kmeans.cluster_centers_,
                                        columns=features.columns)
        kmeans_centroids['n_individuals'] = (kmeans_ind
                                             .groupby('Xclust')
                                             .count())['u_total_modif'] # arbitrary column name
        # Save the kmeans results into a hdf5 file
        path = self.output().path
        kmeans_ind.to_hdf(path, '/individuals')
        kmeans_centroids.to_hdf(path, '/centroids')


class KMeansReport(luigi.Task):
    """Full automatic KMeans with a report.

    Generated report gives the number of components for the PCA and the number
    of clusters for the KMeans.

    As you need to compute all KMeans between 'nbmin_clusters' and
    'nbmax_clusters' (luigi parameters), this task can be used to make several
    KMeans with a different number of clusters.

    Features are normalized, scaled and then reduced by PCA before running a few
    KMeans.

    Select the number of components for the PCA, compute N KMeans and choose the
    optimal number of clusters according to the elbow method.

    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")
    metadata_type = luigi.Parameter("user")
    nbmin_clusters = luigi.parameter.IntParameter(3)
    nbmax_clusters = luigi.parameter.IntParameter(8)

    def outputpath(self):
        fname = "-".join([self.metadata_type, "metadata",
                          "auto-kmeans-report.txt"])
        return osp.join(self.datarep, OUTPUT_DIR, self.dsname, fname)

    def output(self):
        return luigi.LocalTarget(self.outputpath(), format=UTF8)

    def requires(self):
        # Note: n_components=0 for the automatic selection of the number of PCA components
        return {k: KMeansFromPCA(self.datarep, self.dsname, self.metadata_type,
                                 n_components=0, nb_clusters=k)
                for k in range(self.nbmin_clusters, self.nbmax_clusters + 1)}

    def run(self):
        centers = []
        features = []
        labels = []
        for k in range(self.nbmin_clusters, self.nbmax_clusters + 1):
            fpath = self.input()[k].path
            df = pd.read_hdf(fpath, "/individuals")
            center = pd.read_hdf(fpath, "/centroids")
            if "n_individuals" in center:
                center = center.drop("n_individuals", axis=1)
            centers.append(center.values)
            features.append(df.drop("Xclust", axis=1).values)
            labels.append(df['Xclust'].copy().values)
        res = ul.compute_nb_clusters(features, centers,  labels, self.nbmin_clusters)
        with self.output().open('w') as fobj:
            fobj.write("For *{}* metadata\n\n".format(self.metadata_type))
            fobj.write("- PCA components: {}\n".format(df.shape[1] - 1))
            fobj.write("- Nclusters: {}\n".format(res))


class KMeansAnalaysis(luigi.Task):
    """Some KMeans analysis

    Inertia, elbow and silhouette computations in order to choose the number of clusters.
    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")
    metadata_type = luigi.Parameter("user")
    nbmin_clusters = luigi.parameter.IntParameter(3)
    nbmax_clusters = luigi.parameter.IntParameter(8)

    def outputpath(self):
        fname = "-".join([self.metadata_type, "metadata",
                          "kmeans-analysis-elbow-silhouette.png"])
        return osp.join(self.datarep, OUTPUT_DIR, self.dsname, fname)

    def output(self):
        return luigi.LocalTarget(self.outputpath(), format=MixedUnicodeBytes)

    def requires(self):
        # Note: n_components=0 for the automatic selection of the number of PCA components
        return {k: KMeansFromPCA(self.datarep, self.dsname, self.metadata_type,
                                 n_components=0, nb_clusters=k)
                for k in range(self.nbmin_clusters, self.nbmax_clusters + 1)}

    def run(self):
        centers = []
        features = []
        labels = []
        for k in range(self.nbmin_clusters, self.nbmax_clusters + 1):
            fpath = self.input()[k].path
            df = pd.read_hdf(fpath, "/individuals")
            center = pd.read_hdf(fpath, "/centroids")
            centers.append(center.drop("n_individuals", axis=1).values)
            features.append(df.drop("Xclust", axis=1).values)
            labels.append(df['Xclust'].copy().values)
        fig = ul.kmeans_elbow_silhouette(features, centers, labels,
                                         self.nbmin_clusters, self.nbmax_clusters)
        fig.savefig(self.output().path)
