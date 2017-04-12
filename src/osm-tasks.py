# coding: utf-8

""" Luigi implementation for OSM data analysis
"""

import luigi
from luigi.format import UTF8

import pandas as pd
import numpy as np

import osmparsing
import tagmetanalyse

class OSMHistoryParsing(luigi.Task):

    """ Luigi task : parse OSM data history from a .pbf file
    """
    datarep = luigi.Parameter("../data")
    dsname = luigi.Parameter("bordeaux-metropole")

    def outputpath(self):
        return (self.datarep + "/output-extracts/" + self.dsname + "/" +
                self.dsname + "-elements.csv")

    def output(self):
        return luigi.LocalTarget(self.outputpath())

    def run(self):
        tlhandler = osmparsing.TimelineHandler()
        datapath = self.datarep + "/raw/" + self.dsname + ".osh.pbf"
        tlhandler.apply_file(datapath)
        colnames = ['id', 'version', 'visible', 'ts', 'uid',
                'chgset', 'ntags', 'tagkeys', 'elem', 'descr']
        elements = pd.DataFrame(tlhandler.elemtimeline, columns=colnames)
        elements = elements.sort_values(by=['elem', 'id', 'version'])
        print("There are {0} items in the OSM history.".format(len(elements)))
        print("Write OSM history data into {0}".format(self.outputpath()))
        with self.output().open('w') as outputflow:
            elements.to_csv(outputflow, date_format='%Y-%m-%d %H:%M:%S')


class OSMTagParsing(luigi.Task):

    """ Luigi task : parse OSM tag genome from a .pbf file
    """
    datarep = luigi.Parameter("../data")
    dsname = luigi.Parameter("bordeaux-metropole")

    def outputpath(self):
        return (self.datarep + "/output-extracts/" + self.dsname + "/" +
                self.dsname + "-tag-genome.csv")

    def output(self):
        return luigi.LocalTarget(self.outputpath())

    def run(self):
        taghandler = osmparsing.TagGenomeHandler()
        datapath = self.datarep + "/raw/" + self.dsname + ".osh.pbf"
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
    datarep = luigi.Parameter("../data")
    dsname = luigi.Parameter("bordeaux-metropole")

    def outputpath(self):
        return (self.datarep + "/output-extracts/" + self.dsname + "/" +
                self.dsname + "-fulltag-analysis.csv")

    def output(self):
        return luigi.LocalTarget(self.outputpath())

    def requires(self):
        datapath = self.datarep + "/raw/" + self.dsname + ".osh.pbf"
        elempath = (self.datarep + "/output-extracts/" + self.dsname + "/" +
                      self.dsname + "-elements.csv")
        taggenomepath = (self.datarep + "/output-extracts/" + self.dsname +
                         "/" + self.dsname + "-tag-genome.csv")
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
    datarep = luigi.Parameter("../data")
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
    datarep = luigi.Parameter("../data")
    dsname = luigi.Parameter("bordeaux-metropole")

    def outputpath(self):
        return (self.datarep + "/output-extracts/" + self.dsname + "/" +
                self.dsname + "-chgset-md.csv")

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
    datarep = luigi.Parameter("../data")
    dsname = luigi.Parameter("bordeaux-metropole")

    def outputpath(self):
        return (self.datarep + "/output-extracts/" + self.dsname + "/" +
                self.dsname + "-enriched-elements.csv")

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

        # Maximal version of each elements
        osmelem_vmax = (osm_elements.groupby(['elem','id'])['version']
                        .max()
                        .reset_index())
        osmelem_vmax.columns = ['elem','id','vmax']
        osm_elements = pd.merge(osm_elements, osmelem_vmax, on=['elem','id'])

        # Whether or not an elements is up-to-date
        osm_elements['up_to_date'] = osm_elements.version == osm_elements.vmax

        # Whether or not an element will be corrected by another user
        osm_elements['willbe_corr'] = np.logical_and(osm_elements.id[1:] ==
                                                     osm_elements.id[:-1],
                                                     osm_elements.uid[1:] !=
                                                     osm_elements.uid[:-1])
        osm_elements['willbe_corr'] = osm_elements.willbe_corr.shift(-1)
        osm_elements.willbe_corr[-1:] = False
        osm_elements.willbe_corr = osm_elements.willbe_corr.astype('bool')

        # Whether or not an element will be corrected by the same user
        osm_elements['willbe_autocorr'] = np.logical_and(osm_elements.id[1:] ==
                                                         osm_elements.id[:-1],
                                                         osm_elements.uid[1:] ==
                                                         osm_elements.uid[:-1])
        osm_elements['willbe_autocorr'] = osm_elements.willbe_autocorr.shift(-1)
        osm_elements.willbe_autocorr[-1:] = False
        osm_elements.willbe_autocorr = osm_elements.willbe_autocorr.astype('bool')

        # Time before the next modification
        osm_elements['time_before_nextmodif'] = osm_elements.ts.diff()
        osm_elements['time_before_nextmodif'] = (osm_elements.time_before_nextmodif
                                                 .shift(-1))
        osm_elements.loc[osm_elements.up_to_date,['time_before_nextmodif']] = pd.NaT

        # Time before the next modification, if it is done by another user
        osm_elements['time_before_nextcorr'] = osm_elements.time_before_nextmodif
        osm_elements['time_before_nextcorr']=osm_elements.time_before_nextcorr.where(osm_elements.willbe_corr,other=pd.NaT)

        # Time before the next modification, if it is done by the same user
        osm_elements['time_before_nextauto'] = osm_elements.time_before_nextmodif
        osm_elements['time_before_nextauto'] = (osm_elements.time_before_nextauto
                                                .where(osm_elements.willbe_autocorr,
                                                       other=pd.NaT))

        with self.output().open('w') as outputflow:
            osm_elements.to_csv(outputflow, date_format='%Y-%m-%d %H:%M:%S')

class UserMetadataExtract(luigi.Task):
    """ Luigi task: extraction of metadata for each OSM user
    """
    datarep = luigi.Parameter("../data")
    dsname = luigi.Parameter("bordeaux-metropole")

    def outputpath(self):
        return (self.datarep + "/output-extracts/" + self.dsname + "/" +
                self.dsname + "-user-md.csv")

    def output(self):
        return luigi.LocalTarget(self.outputpath())

    def requires(self):
        return OSMHistoryParsing(self.datarep, self.dsname)

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
            chgset_md.user_lastchgset = pd.to_timedelta(chgset_md.user_lastchgset)
        with self.input()['enrichhist'].open('r') as inputflow:
            osm_elements = pd.read_csv(inputflow,
                                       index_col=0,
                                       parse_dates=['ts'])
            osm_elements.time_before_nextmodif = pd.to_timedelta(osm_elements.time_before_nextmodif)
            osm_elements.time_before_nextcorr = pd.to_timedelta(osm_elements.time_before_nextcorr)
            osm_elements.time_before_nextauto = pd.to_timedelta(osm_elements.time_before_nextauto)

        user_md = (osm_elements.groupby('uid')['chgset']
                   .nunique()
                   .reset_index())
        user_md = pd.merge(user_md, (chgset_md
                                     .groupby('uid')['n_modif']
                                     .agg({'nmed_modif_bychgset':"median",
                                           'nmax_modif_bychgset':"max"})
                                     .reset_index()))
        user_md = pd.merge(user_md, (chgset_md
                                     .groupby('uid')['n_uniqelem']
                                     .agg({'nmed_elem_bychgset':"median",
                                           'nmax_elem_bychgset':"max"})
                                     .reset_index()))
        user_md['meantime_between_chgset'] = (chgset_md
                                              .groupby('uid')['user_lastchgset']
                                              .apply(lambda x: x.mean())
                                              .reset_index()['user_lastchgset'])

        #
        user_md['n_modif'] = (osm_elements.groupby('uid')['elem']
                              .count()
                              .reset_index()['elem'])
        user_md_byelem = (osm_elements.groupby(['uid','elem'])['id']
                          .count()
                          .unstack()
                          .reset_index()
                          .fillna(0))
        user_md = pd.merge(user_md, user_md_byelem, on='uid').fillna(0)
        user_md.columns.values[-4:] = ['n_modif', 'n_nodemodif',
                                       'n_relationmodif', 'n_waymodif']

        #
        contrib_byelem = (osm_elements.groupby(['elem', 'id', 'uid'])['version']
                          .count()
                          .reset_index())
        user_md['nmed_modif_byelem'] = (contrib_byelem.groupby('uid')['version']
                                        .median()
                                        .reset_index()['version'])
        user_md_byelem = (contrib_byelem.groupby(['uid','elem'])['version']
                          .median()
                          .unstack()
                          .reset_index()
                          .fillna(0))
        user_md = pd.merge(user_md, user_md_byelem, on='uid').fillna(0)
        user_md.columns.values[-3:] = ['nmed_modif_bynode',
                                       'nmed_modif_byrelation',
                                       'nmed_modif_byway']
        user_md_byelem = (contrib_byelem.groupby(['uid','elem'])['version']
                          .max()
                          .unstack()
                          .reset_index()
                          .fillna(0))
        user_md = pd.merge(user_md, user_md_byelem, on='uid').fillna(0)
        user_md.columns.values[-3:] = ['nmax_modif_bynode',
                                       'nmax_modif_byrelation',
                                       'nmax_modif_byway']

        #
        user_md['first_at'] = (osm_elements.groupby('uid')['ts']
                               .min()
                               .reset_index()['ts'])
        user_md['last_at'] = (osm_elements.groupby('uid')['ts']
                              .max()
                              .reset_index()['ts'])
        user_md['activity'] = user_md.last_at - user_md.first_at

        #
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

        #
        user_md_byelem = (osm_elements.groupby(['uid','elem'])['id']
                          .nunique()
                          .unstack()
                          .reset_index()
                          .fillna(0))
        user_md_byelem['n_elem'] = (user_md_byelem[['node','relation','way']]
                                    .apply(sum, axis=1))
        user_md = pd.merge(user_md, user_md_byelem, on='uid', how="outer").fillna(0)
        user_md.columns.values[-4:] = ['n_node', 'n_relation', 'n_way', 'n_elem']

        #
        osmelem_last_byuser = (osm_elements.groupby(['elem','id','uid'])['version']
                               .last()
                               .reset_index())
        osmelem_last_byuser = pd.merge(osmelem_last_byuser,
                                       osm_elements[['elem','uid','id','version',
                                                     'visible','up_to_date']],
                                       on=['elem','id','uid','version'])
        lastcontrib_byuser = osm_elements.query("up_to_date")[['elem','id','uid']]
        osmelem_last_byuser = pd.merge(osmelem_last_byuser, lastcontrib_byuser,
                                       on=['elem','id'])
        osmelem_last_byuser.columns = ['elem','id','uid','version','visible',
                                       'up_to_date','lastuid']

        #
        osmelem_lastcontrib = osmelem_last_byuser.query("up_to_date")
        osmelem_lastdel = osmelem_lastcontrib.query("not visible")
        user_md_byelem = (osmelem_lastdel.groupby(['uid','elem'])['id']
                          .count()
                          .unstack()
                          .reset_index()
                          .fillna(0))
        user_md_byelem['elem'] = (user_md_byelem[['node','relation','way']]
                                  .apply(sum, axis=1))
        user_md = pd.merge(user_md, user_md_byelem, on='uid', how="outer")
        user_md.columns.values[-4:] = ['n_node_lastdel', 'n_relation_lastdel',
                                       'n_way_lastdel', 'n_elem_lastdel']

        #
        osmelem_lastupd = osmelem_lastcontrib.query("visible")
        user_md_byelem = (osmelem_lastupd.groupby(['uid','elem'])['id']
                          .count()
                          .unstack()
                          .reset_index()
                          .fillna(0))
        user_md_byelem['elem'] = (user_md_byelem[['node','relation','way']]
                                  .apply(sum, axis=1))
        user_md = pd.merge(user_md, user_md_byelem, on='uid', how="outer").fillna(0)
        user_md.columns.values[-4:] = ['n_node_lastupd', 'n_relation_lastupd',
                                       'n_way_lastupd', 'n_elem_lastupd']

        #
        osmelem_last_byuser['old_contrib'] = np.logical_and(
            ~(osmelem_last_byuser.up_to_date),
            osmelem_last_byuser.uid != osmelem_last_byuser.lastuid)
        osmelem_oldcontrib = osmelem_last_byuser.query("old_contrib")
        osmelem_olddel = osmelem_oldcontrib.query("not visible")
        user_md_byelem = (osmelem_olddel.groupby(['uid','elem'])['id']
                          .count()
                          .unstack()
                          .reset_index()
                          .fillna(0))
        user_md_byelem['elem'] = (user_md_byelem[['node','relation','way']]
                                  .apply(sum, axis=1))
        user_md = pd.merge(user_md, user_md_byelem, on='uid', how="outer").fillna(0)
        user_md.columns.values[-4:] = ['n_node_olddel', 'n_relation_olddel',
                                       'n_way_olddel', 'n_elem_olddel']

        #
        osmelem_oldupd = osmelem_oldcontrib.query("visible")
        user_md_byelem = (osmelem_oldupd.groupby(['uid','elem'])['id']
                          .count()
                          .unstack()
                          .reset_index()
                          .fillna(0))
        user_md_byelem['elem'] = (user_md_byelem[['node','relation','way']]
                                  .apply(sum, axis=1))
        user_md = pd.merge(user_md, user_md_byelem, on='uid', how="outer").fillna(0)
        user_md.columns.values[-4:] = ['n_node_oldupd', 'n_relation_oldupd',
                                       'n_way_oldupd', 'n_elem_oldupd']
        #
        osmelem_creation = (osm_elements.groupby(['elem','id'])['version']
                            .agg({'first':"first",'last':"last"})
                            .stack()
                            .reset_index())
        osmelem_creation.columns = ['elem','id','step','version']
        osmelem_creation = pd.merge(osmelem_creation,
                                    osm_elements[['elem','uid','id','version',
                                                  'visible','up_to_date']],
                                    on=['elem','id','version'])
        #
        osmelem_cr = osmelem_creation.query("step == 'first'")
        user_md_byelem = (osmelem_cr.groupby(['uid','elem'])['id']
                          .count()
                          .unstack()
                          .reset_index()
                          .fillna(0))
        user_md_byelem['elem'] = (user_md_byelem[['node','relation','way']]
                                  .apply(sum, axis=1))
        user_md = pd.merge(user_md, user_md_byelem, on='uid', how="outer").fillna(0)
        user_md.columns.values[-4:] = ['n_node_cr', 'n_relation_cr',
                                       'n_way_cr', 'n_elem_cr']
        #
        osmelem_cr_upd = osmelem_cr.loc[osmelem_cr.up_to_date]
        user_md_byelem = (osmelem_cr_upd.groupby(['uid','elem'])['id']
                          .count()
                          .unstack()
                          .reset_index()
                          .fillna(0))
        user_md_byelem['elem'] = (user_md_byelem[['node','relation','way']]
                                  .apply(sum, axis=1))
        user_md = pd.merge(user_md, user_md_byelem, on='uid', how="outer").fillna(0)
        user_md.columns.values[-4:] = ['n_node_crupd', 'n_relation_crupd',
                                       'n_way_crupd', 'n_elem_crupd']
        #
        osmelem_cr_old = osmelem_cr.query("not up_to_date")
        osmelem_last = osmelem_creation.query("step == 'last'")
        osmelem_cr_old = pd.merge(osmelem_cr_old,
                                  osmelem_last[['elem','id','visible']],
                                  on=['elem','id'])
        osmelem_cr_mod = osmelem_cr_old.query("visible_y")
        user_md_byelem = (osmelem_cr_mod.groupby(['uid','elem'])['id']
                          .count()
                          .unstack()
                          .reset_index()
                          .fillna(0))
        user_md_byelem['elem'] = (user_md_byelem[['node','relation','way']]
                                  .apply(sum, axis=1))
        user_md = pd.merge(user_md, user_md_byelem, on='uid', how="outer").fillna(0)
        user_md.columns.values[-4:] = ['n_node_crmod', 'n_relation_crmod',
                                       'n_way_crmod', 'n_elem_crmod']
        #
        osmelem_cr_del = osmelem_cr_old.query("not visible_y")
        user_md_byelem = (osmelem_cr_del.groupby(['uid','elem'])['id']
                          .count()
                          .unstack()
                          .reset_index()
                          .fillna(0))
        user_md_byelem['elem'] = (user_md_byelem[['node','relation','way']]
                                  .apply(sum, axis=1))
        user_md = pd.merge(user_md, user_md_byelem, on='uid', how="outer").fillna(0)
        user_md.columns.values[-4:] = ['n_node_crdel', 'n_relation_crdel',
                                       'n_way_crdel', 'n_elem_crdel']


        with self.output().open('w') as outputflow:
            user_md.to_csv(outputflow, date_format='%Y-%m-%d %H:%M:%S')
