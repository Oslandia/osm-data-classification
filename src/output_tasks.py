# coding: utf-8

""" Luigi implementation for OSM data analysis
"""

import os.path as osp

import luigi
from luigi.format import MixedUnicodeBytes, UTF8

import analysis_task

class OSMTagMetaAnalysis(luigi.Task):
    """ Luigi task: generic task that implements the tag meta-analysis
    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")
    
    def requires(self):
        yield OSMTagCount(self.datarep, self.dsname)
        yield OSMTagKeyCount(self.datarep, self.dsname)
        yield OSMTagFreq(self.datarep, self.dsname)
        yield OSMTagValue(self.datarep, self.dsname)
        yield OSMTagValueFreq(self.datarep, self.dsname)

class MasterTask(luigi.Task):
    """ Luigi task: generic task that launches every final tasks
    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")

    def requires(self):
        yield ElementMetadataExtract(self.datarep, self.dsname)
        yield OSMChronology(self.datarep, self.dsname,
                            '2006-01-01', '2017-06-01')
        yield MetadataKmeans(self.datarep, self.dsname, "user", "manual", 3, 10)

    def complete(self):
        return False

