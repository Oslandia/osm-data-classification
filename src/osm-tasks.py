# coding: utf-8

""" Luigi implementation for OSM data analysis
"""

import os.path as osp

import luigi
from luigi.format import UTF8
import pandas as pd
import numpy as np

import osmparsing
import tagmetanalyse
from utils import groupuser_count, groupuser_nunique, groupuser_stats

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
        print("There are {0} items in the OSM history.".format(len(elements)))
        print("Write OSM history data into {0}".format(self.outputpath()))
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
        print("Write OSM tag genome into {0}".format(self.outputpath()))
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
        print("Recovering OSM elements: {0} elements in this dataset!"
              .format(len(osm_elements)))
        with self.input()['taggenome'].open('r') as inputflow:
            tag_genome = pd.read_csv(inputflow, index_col=0)
        print("Recovering OSM tag genome: {0} tag items!".format(len(tag_genome)))
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
        print(tagkeycount.head(10))

        ### Analyse of tag key frequency (amongst all elements)
        fulltaganalys = pd.merge(osm_elements[['elem', 'id', 'version']],
                                 tag_genome,
                                 on=['elem','id','version'],
                                 how="outer")
        tagfreq = tagmetanalyse.tag_frequency(fulltaganalys, ['elem','version'])
        # When are elements tagged with 'highway' keys?
        print(tagfreq.loc['highway'])

        ### Analyse of tag value frequency (amongst all tagged elements)
        tagmetanalyse.tagvalue_analysis(tag_genome, 'highway', ['version'])

        ### Analyse of tag value frequency (amongst all tagged element)
        # For element with a specific tag key (e.g. 'highway')
        tagvalue_freq = tagmetanalyse.tagvalue_frequency(tag_genome, "highway",
                                                         ['elem','version'])

        # How many elements are tagged with value 'residential', amongst
        # "highway" elements?
        print(tagvalue_freq.loc['residential',0:10])

        with self.output().open('w') as outputflow:
            fulltaganalys.to_csv(outputflow, date_format='%Y-%m-%d %H:%M:%S')


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
        return OSMHistoryParsing(self.datarep, self.dsname)

    def run(self):
        with self.input().open('r') as inputflow:
            osm_elements = pd.read_csv(inputflow,
                                       index_col=0,
                                       parse_dates=['ts'])
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
        return OSMHistoryParsing(self.datarep, self.dsname)

    def run(self):
        print("Extraction of change sets metadata...")
        with self.input().open('r') as inputflow:
            osm_elements = pd.read_csv(inputflow,
                                       index_col=0,
                                       parse_dates=['ts'])
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
        with self.output().open('w') as outputflow:
            chgset_md.to_csv(outputflow, date_format='%Y-%m-%d %H:%M:%S')

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
        print("Feature extraction from OSM history data...")
        with self.input().open('r') as inputflow:
            osm_elements = pd.read_csv(inputflow,
                                       index_col=0,
                                       parse_dates=['ts'])
        osm_elements.sort_values(by=['elem','id','version'])
        
        # Extract information from first and last versions
        osmelem_first_version = (osm_elements
                                 .groupby(['elem','id'])['version', 'uid']
                                 .first()
                                 .reset_index())
        osm_elements = pd.merge(osm_elements, osmelem_first_version,
                                    on=['elem','id'])
        osm_elements.columns = ['elem', 'id', 'version', 'visible', 'ts',
                                    'uid', 'chgset', 'ntags', 'tagkeys',
                                    'vmin', 'first_uid']
        osmelem_last_version = (osm_elements
                                 .groupby(['elem','id'])['version', 'uid']
                                 .last()
                                 .reset_index())
        osm_elements = pd.merge(osm_elements, osmelem_last_version,
                                    on=['elem','id'])
        osm_elements.columns = ['elem', 'id', 'version', 'visible', 'ts',
                                    'uid', 'chgset', 'ntags', 'tagkeys',
                                    'vmin', 'first_uid', 'vmax', 'last_uid']

        # New version-related features
        osm_elements['init'] = osm_elements.version == osm_elements.vmin
        osm_elements['up_to_date'] = osm_elements.version == osm_elements.vmax
        osm_elements = osm_elements.drop(['vmin', 'vmax'], axis=1)

        # Whether or not an element is corrected in the current version
        osm_elements['is_corr'] = np.logical_and(osm_elements.id.diff()==0,
                                                 osm_elements.uid.diff()!=0)
        osm_elements['is_autocorr'] = np.logical_and(osm_elements.id.diff()==0,
                                                 osm_elements.uid.diff()==0)
        
        # Whether or not an element will be corrected in the last version
        osm_elements['willbe_corr'] = np.logical_and(osm_elements.id.diff(-1)==0,
                                                  osm_elements.uid.diff(-1)!=0)        
        osm_elements['willbe_autocorr'] = np.logical_and(osm_elements.id
                                                         .diff(-1)==0,
                                                         osm_elements.uid
                                                         .diff(-1)==0)
        
        # Time before the next modification
        osm_elements['nextmodif_in'] = - osm_elements.ts.diff(-1)
        osm_elements.loc[osm_elements.up_to_date,['nextmodif_in']] = pd.NaT

        # Time before the next modification, if it is done by another user
        osm_elements['nextcorr_in'] = osm_elements.nextmodif_in
        osm_elements['nextcorr_in'] = (osm_elements.nextcorr_in
                                       .where(osm_elements.willbe_corr,
                                              other=pd.NaT))

        # Time before the next modification, if it is done by the same user
        osm_elements['nextauto_in'] = osm_elements.nextmodif_in
        osm_elements['nextauto_in'] = (osm_elements.nextauto_in
                                       .where(osm_elements.willbe_autocorr,
                                                       other=pd.NaT))
        osm_elements = osm_elements.drop(['willbe_corr', 'willbe_autocorr'],
                                         axis=1)

        # Element saving
        with self.output().open('w') as outputflow:
            osm_elements.to_csv(outputflow, date_format='%Y-%m-%d %H:%M:%S')

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
        print("Extraction of user metadata...")
        with self.input()['chgsets'].open('r') as inputflow:
            chgset_md = pd.read_csv(inputflow,
                                    index_col=0)
            chgset_md.user_lastchgset = pd.to_timedelta(chgset_md
                                                        .user_lastchgset)
        with self.input()['enrichhist'].open('r') as inputflow:
            osm_elements = pd.read_csv(inputflow,
                                       index_col=0,
                                       parse_dates=['ts'])
            osm_elements.nextmodif_in = pd.to_timedelta(osm_elements
                                                        .nextmodif_in)
            osm_elements.nextcorr_in = pd.to_timedelta(osm_elements.nextcorr_in)
            osm_elements.nextauto_in = pd.to_timedelta(osm_elements.nextauto_in)

        # Basic features: nb change sets, timestamps
        user_md = (osm_elements.groupby('uid')['chgset']
                   .nunique()
                   .reset_index())
        user_md.colnames = ['uid', 'n_chgset']
        user_md['first_at'] = (osm_elements.groupby('uid')['ts']
                               .min()
                               .reset_index()['ts'])
        user_md['last_at'] = (osm_elements.groupby('uid')['ts']
                              .max()
                              .reset_index()['ts'])
        user_md['activity'] = user_md.last_at - user_md.first_at
        # Change set-related features
        user_md = groupuser_stats(user_md, chgset_md, 'uid', 'n_modif',
                                  'modif_bychgset')
        user_md = groupuser_stats(user_md, chgset_md, 'uid', 'n_uniqelem',
                                  'elem_bychgset')
        user_md['meantime_between_chgset'] = (chgset_md
                                              .groupby('uid')['user_lastchgset']
                                              .apply(lambda x: x.mean())
                                              .reset_index()['user_lastchgset'])

        # Total number of modifications
        user_md = groupuser_count(user_md, osm_elements, 'uid', 'id', '_modif')

        # Number of modifications per unique element
        contrib_byelem = (osm_elements.groupby(['elem', 'id', 'uid'])['version']
                          .count()
                          .reset_index())
        user_md = groupuser_stats(user_md, contrib_byelem, 'uid', 'version',
                                  'modif_byelem')

        # Update times
        user_md['update_medtime'] = (osm_elements
                                     .groupby('uid')['nextmodif_in']
                                     .apply(lambda x: x.median())
                                     .reset_index()['nextmodif_in'])
        user_md['corr_medtime'] = (osm_elements
                                   .groupby('uid')['nextcorr_in']
                                   .apply(lambda x: x.median())
                                   .reset_index()['nextcorr_in'])
        user_md['autocorr_medtime'] = (osm_elements
                                       .groupby('uid')['nextauto_in']
                                       .apply(lambda x: x.median())
                                       .reset_index()['nextauto_in'])

        # Number of unique elements that received a contribution
        user_md = groupuser_nunique(user_md, osm_elements, 'uid', 'id', '')

        # Number of elements for which the user is the last contributor
        osmelem_last_byuser = (osm_elements
                               .groupby(['elem','id','uid'])['version']
                               .last()
                               .reset_index())
        osmelem_last_byuser = pd.merge(osmelem_last_byuser,
                                       osm_elements[['elem','uid',
                                                     'id','version',
                                                     'visible','up_to_date']],
                                       on=['elem','id','uid','version'])
        lastcontrib_byuser = osm_elements.query("up_to_date")[['elem',
                                                               'id',
                                                               'uid']]
        osmelem_last_byuser = pd.merge(osmelem_last_byuser, lastcontrib_byuser,
                                       on=['elem','id'])
        osmelem_last_byuser.columns = ['elem','id','uid','version','visible',
                                       'up_to_date','lastuid']
        osmelem_lastcontrib = osmelem_last_byuser.query("up_to_date")
        osmelem_lastdel = osmelem_lastcontrib.query("not visible")
        user_md = groupuser_count(user_md, osmelem_lastdel, 'uid', 'id',
                                  '_lastdel')
        osmelem_lastutd = osmelem_lastcontrib.query("visible")
        user_md = groupuser_count(user_md, osmelem_lastutd, 'uid', 'id',
                                  '_lastavl')

        # Number of contribution done by the user and updated since
        osmelem_last_byuser['old_contrib'] = np.logical_and(
            ~(osmelem_last_byuser.up_to_date), osmelem_last_byuser.uid !=
            osmelem_last_byuser.lastuid)
        osmelem_oldcontrib = osmelem_last_byuser.query("old_contrib")
        osmelem_olddel = osmelem_oldcontrib.query("not visible")
        user_md = groupuser_count(user_md, osmelem_olddel, 'uid', 'id',
                                  '_olddel')
        osmelem_oldmod = osmelem_oldcontrib.query("visible")
        user_md = groupuser_count(user_md, osmelem_oldmod, 'uid', 'id',
        '_oldmod')

        # Number of created elements
        osmelem_creation = (osm_elements.groupby(['elem','id'])['version']
                            .agg({'first':"first",'last':"last"})
                            .stack()
                            .reset_index())
        osmelem_creation.columns = ['elem','id','step','version']
        osmelem_creation = pd.merge(osmelem_creation,
                                    osm_elements[['elem','uid','id','version',
                                                  'visible','up_to_date']],
                                    on=['elem','id','version'])
        osmelem_cr = osmelem_creation.query("step == 'first'")
        user_md = groupuser_count(user_md, osmelem_cr, 'uid', 'id', '_cr')
        osmelem_cr_utd = osmelem_cr.loc[osmelem_cr.up_to_date]
        user_md = groupuser_count(user_md, osmelem_cr_utd, 'uid', 'id',
                                  '_crutd')
        osmelem_cr_old = osmelem_cr.query("not up_to_date")
        osmelem_last = osmelem_creation.query("step == 'last'")
        osmelem_cr_old = pd.merge(osmelem_cr_old,
                                  osmelem_last[['elem','id','visible']],
                                  on=['elem','id'])
        osmelem_cr_mod = osmelem_cr_old.query("visible_y")
        user_md = groupuser_count(user_md, osmelem_cr_mod, 'uid', 'id', '_crmod')
        osmelem_cr_del = osmelem_cr_old.query("not visible_y")
        user_md = groupuser_count(user_md, osmelem_cr_del, 'uid', 'id', '_crdel')

        # Metadata saving
        with self.output().open('w') as outputflow:
            user_md.to_csv(outputflow, date_format='%Y-%m-%d %H:%M:%S')

class MasterTask(luigi.Task):
    """ Luigi task: generic task that launches every final tasks
    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")
    
    def requires(self):
        yield UserMetadataExtract(self.datarep, self.dsname)
        yield ElementMetadataExtract(self.datarep, self.dsname)
        yield OSMTagMetaAnalysis(self.datarep, self.dsname)
