# coding: utf-8

""" Luigi implementation for OSM data analysis
"""

import os.path as osp
from datetime import date

import luigi
from luigi.format import MixedUnicodeBytes, UTF8

import analysis_tasks

class OSMTagMetaAnalysis(luigi.Task):
    """ Luigi task: generic task that implements the tag meta-analysis
    """
    datarep = luigi.Parameter(default="data")
    dsname = luigi.Parameter()

    def requires(self):
        yield analysis_tasks.OSMTagCount(self.datarep, self.dsname)
        yield analysis_tasks.OSMTagKeyCount(self.datarep, self.dsname)
        yield analysis_tasks.OSMTagFreq(self.datarep, self.dsname)
        yield analysis_tasks.OSMTagValue(self.datarep, self.dsname)
        yield analysis_tasks.OSMTagValueFreq(self.datarep, self.dsname)

class MasterTask(luigi.Task):
    """ Luigi task: generic task that launches every final tasks
    """
    datarep = luigi.Parameter(default="data")
    dsname = luigi.Parameter()
    select_param = luigi.Parameter(default='manual')
    end_date = luigi.DateParameter(default=date.today())

    def requires(self):
        yield analysis_tasks.ElementMetadataExtract(self.datarep, self.dsname)
        yield analysis_tasks.OSMChronology(self.datarep, self.dsname,
                                           end_date=self.end_date)
        yield analysis_tasks.PlottingClusteredIndiv(self.datarep, self.dsname)

    def complete(self):
        return False
