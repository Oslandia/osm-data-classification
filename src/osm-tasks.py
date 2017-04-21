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
from utils import group_count, group_nunique, group_stats

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
                                 .groupby(['elem','id'])['version', 'uid',
                                                         'visible']
                                 .last()
                                 .reset_index())
        osm_elements = pd.merge(osm_elements, osmelem_last_version,
                                    on=['elem','id'])
        osm_elements.columns = ['elem', 'id', 'version', 'visible', 'ts',
                                'uid', 'chgset', 'ntags', 'tagkeys', 'vmin',
                                'first_uid', 'vmax', 'last_uid', 'available']

        # New version-related features
        osm_elements['init'] = osm_elements.version == osm_elements.vmin
        osm_elements['up_to_date'] = osm_elements.version == osm_elements.vmax
        osm_elements = osm_elements.drop(['vmin'], axis=1)
        
        # Whether or not an element will be corrected in the last version
        osm_elements['willbe_corr'] = np.logical_and(osm_elements.id
                                                     .diff(-1)==0,
                                                  osm_elements.uid
                                                     .diff(-1)!=0)        
        osm_elements['willbe_autocorr'] = np.logical_and(osm_elements.id
                                                         .diff(-1)==0,
                                                         osm_elements.uid
                                                         .diff(-1)==0)
        
        # Time before the next modification
        osm_elements['nextmodif_in'] = - osm_elements.ts.diff(-1)
        osm_elements.loc[osm_elements.up_to_date,['nextmodif_in']] = pd.NaT
        osm_elements.nextmodif_in = (osm_elements.nextmodif_in
                                     .astype('timedelta64[h]'))

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
        
        # Element saving
        with self.output().open('w') as outputflow:
            osm_elements.to_csv(outputflow, date_format='%Y-%m-%d %H:%M:%S')

            
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
        # Basic features: change set identification, time-related features
        chgset_md = (osm_elements.groupby(['chgset', 'uid'])['elem']
                    .count().reset_index())
        chgset_md['opened_at'] = (osm_elements.groupby('chgset')['ts']
                                  .min()
                                  .reset_index()['ts'])
        chgset_md.sort_values(by=['uid','opened_at'], inplace=True)
        chgset_md['user_lastchgset_inhour'] = (chgset_md.groupby('uid')['opened_at']
                                        .diff())
        chgset_md.user_lastchgset_inhour = (chgset_md.user_lastchgset_inhour
                                            .astype('timedelta64[h]'))
        chgset_md['lastmodif_at'] = (osm_elements.groupby('chgset')['ts']
                                     .max()
                                     .reset_index()['ts'])
        chgset_md['duration_insec'] = (chgset_md.lastmodif_at
                                       - chgset_md.opened_at)
        chgset_md.duration_insec = (chgset_md.duration_insec
                                    .astype('timedelta64[s]'))

        # Modification-related features
        chgset_md = group_count(chgset_md, osm_elements, 'chgset', 'id',
                                '_modif')
        osmmodif_cr = osm_elements.query("init")        
        chgset_md = group_count(chgset_md, osmmodif_cr, 'chgset', 'id',
                                  '_modif_cr')
        osmmodif_del = osm_elements.query("not init and not visible")
        chgset_md = group_count(chgset_md, osmmodif_del, 'chgset', 'id',
                                  '_modif_del')
        osmmodif_imp = osm_elements.query("not init and visible")
        chgset_md = group_count(chgset_md, osmmodif_imp, 'chgset', 'id',
                                  '_modif_imp')

        # Number of modifications per unique element
        contrib_byelem = (osm_elements.groupby(['elem', 'id', 'chgset'])['version']
                          .count()
                          .reset_index())
        chgset_md = group_stats(chgset_md, contrib_byelem, 'chgset', 'version',
                                'n', '_modif_byelem')
        # Element-related features
        chgset_md = group_nunique(chgset_md, osm_elements, 'chgset', 'id',
                                  '')        
        osmelem_cr = osm_elements.query("init and available")
        chgset_md = group_nunique(chgset_md, osmelem_cr, 'chgset', 'id', '_cr')
        osmelem_imp = osm_elements.query("not init and visible and available")
        chgset_md = group_nunique(chgset_md, osmelem_imp, 'chgset', 'id', '_imp')
        osmelem_del = osm_elements.query("not init and not visible and not available")
        chgset_md = group_nunique(chgset_md, osmelem_del, 'chgset', 'id', '_del')
        
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
        user_md = group_stats(user_md, chgset_md, 'uid',
                              'user_lastchgset_inhour', 't',
                              '_between_chgsets_inhour')
        user_md = group_stats(user_md, chgset_md, 'uid', 'duration_insec',
                                        'd', '_chgset_insec')
        user_md = group_stats(user_md, chgset_md, 'uid', 'n_elem_modif',
                                  'n', '_modif_bychgset')
        user_md = group_stats(user_md, chgset_md, 'uid', 'n_elem',
                                  'n', '_elem_bychgset')

        # Update features
        user_md = group_stats(user_md, osm_elements, 'uid', 'nextmodif_in',
                                  't', '_update_inhour')
        osmelem_corr = osm_elements.query("willbe_corr")
        user_md = group_stats(user_md, osmelem_corr, 'uid', 'nextcorr_in',
                                  't', '_corr_inhour')
        user_md = group_count(user_md, osmelem_corr, 'uid', 'willbe_corr',
                                  '_corr')
        osmelem_autocorr = osm_elements.query("willbe_autocorr")
        user_md = group_stats(user_md, osmelem_autocorr, 'uid',
                                  'nextauto_in', 't', '_autocorr_inhour')
        user_md = group_count(user_md, osmelem_autocorr, 'uid',
                                  'willbe_autocorr', '_autocorr')
        
        # Modification-related features
        user_md = group_count(user_md, osm_elements, 'uid', 'id', '_modif')
        #
        osmmodif_cr = osm_elements.query("init")        
        user_md = group_count(user_md, osmmodif_cr, 'uid', 'id',
                                  '_modif_cr')
        osmmodif_cr_utd = osmmodif_cr.query("up_to_date")
        user_md = group_count(user_md, osmmodif_cr_utd, 'uid', 'id',
                                  '_modif_crutd')
        osmmodif_cr_mod = osmmodif_cr.query("not up_to_date and available")
        user_md = group_count(user_md, osmmodif_cr_mod, 'uid', 'id',
                                  '_modif_crmod')
        osmmodif_cr_del = osmmodif_cr.query("not up_to_date and not available")
        user_md = group_count(user_md, osmmodif_cr_del, 'uid', 'id',
                                  '_modif_crdel')
        #
        osmmodif_del = osm_elements.query("not init and not visible")
        user_md = group_count(user_md, osmmodif_del, 'uid', 'id',
                                  '_modif_del')
        osmmodif_del_utd = osmmodif_del.query("not available")
        user_md = group_count(user_md, osmmodif_del_utd, 'uid', 'id',
                                  '_modif_delutd')
        osmmodif_del_rebirth = osmmodif_del.query("available")
        user_md = group_count(user_md, osmmodif_del_rebirth, 'uid', 'id',
                                  '_modif_delrebirth')
        user_md = group_stats(user_md, osmmodif_del, 'uid', 'version',
                                  'v', '_modif_del')
        #
        osmmodif_imp = osm_elements.query("not init and visible")
        user_md = group_count(user_md, osmmodif_imp, 'uid', 'id',
                                  '_modif_imp')
        osmmodif_imp_utd = osmmodif_imp.query("up_to_date")
        user_md = group_count(user_md, osmmodif_imp_utd, 'uid', 'id',
                                  '_modif_imputd')
        osmmodif_imp_mod = osmmodif_imp.query("not up_to_date and available")
        user_md = group_count(user_md, osmmodif_imp_mod, 'uid', 'id',
                                  '_modif_impmod')
        osmmodif_imp_del = osmmodif_imp.query("not up_to_date and not available")
        user_md = group_count(user_md, osmmodif_imp_del, 'uid', 'id',
                                  '_modif_impdel')
        user_md = group_stats(user_md, osmmodif_imp, 'uid', 'version',
                                  'v', '_modif_imp')
        
        # Number of modifications per unique element
        contrib_byelem = (osm_elements.groupby(['elem', 'id', 'uid'])['version']
                          .count()
                          .reset_index())
        user_md = group_stats(user_md, contrib_byelem, 'uid', 'version',
                                  'n', '_modif_byelem')
        user_md = group_count(user_md, contrib_byelem.query("version==1"),
                                  'uid', 'id', '_with_1_contrib')

        # User-related features
        user_md = group_nunique(user_md, osm_elements, 'uid', 'id', '')
        osmelem_cr = osm_elements.query("init and available")
        user_md = group_nunique(user_md, osmelem_cr, 'uid', 'id', '_cr')
        user_md = group_stats(user_md, osmelem_cr, 'uid', 'vmax',
                                  'v', '_cr')
        osmelem_cr_wrong = osm_elements.query("init and not available")
        user_md = group_nunique(user_md, osmelem_cr_wrong, 'uid', 'id',
                                    '_cr_wrong')
        user_md = group_stats(user_md, osmelem_cr_wrong, 'uid', 'vmax',
                                  'v', '_cr_wrong')
        osmelem_imp = osm_elements.query("not init and visible and available")
        user_md = group_nunique(user_md, osmelem_imp, 'uid', 'id', '_imp')
        user_md = group_stats(user_md, osmelem_imp, 'uid', 'vmax',
                                  'v', '_imp')
        osmelem_imp_wrong = osm_elements.query("not init and visible and not available")
        user_md = group_nunique(user_md, osmelem_imp_wrong, 'uid', 'id', '_imp_wrong')
        user_md = group_stats(user_md, osmelem_imp_wrong, 'uid', 'vmax',
                                  'v', '_imp_wrong')
        osmelem_del = osm_elements.query("not init and not visible and not available")
        user_md = group_nunique(user_md, osmelem_del, 'uid', 'id', '_del')
        user_md = group_stats(user_md, osmelem_del, 'uid', 'vmax',
                                  'v', '_del')
        osmelem_del_wrong = osm_elements.query("not init and not visible and available")
        user_md = group_nunique(user_md, osmelem_del_wrong, 'uid', 'id', '_del_wrong')
        user_md = group_stats(user_md, osmelem_del_wrong, 'uid', 'vmax',
                                  'v', '_del_wrong')
    
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
