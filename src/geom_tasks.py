# coding: utf-8

""" Luigi implementation for OSM data analysis: in-base operations for parsing
OSM metadata and their geometry
"""

import luigi
import luigi.contrib.postgres as lpg
from luigi.format import MixedUnicodeBytes
import pandas as pd

import analysis_tasks

class OSMElementTableCopy(lpg.CopyToTable):
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter("bordeaux-metropole")

    host = "localhost"
    database = "osm"
    user = "rde"
    password = ""
    table = "_".join([dsname.task_value("", "").replace("-", "_"), "elements"])
    columns = [("elem", "varchar"), ("id", "bigint"),
               ("first_at", "timestamp"), ("last_at", "timestamp"),
               ("lifespan", "float"), ("n_inscription_days", "float"),
               ("n_activity_days", "float"), ("version", "int"),
               ("n_chgset", "int"), ("n_user", "int"), ("n_autocorr", "int"),
               ("n_corr", "int"), ("visible", "boolean"), ("first_uid", "int"),
               ("last_uid", "int"), ("first_ug", "int"), ("last_ug", "int")]
    def requires(self):
        return analysis_tasks.ElementMetadataExtract(self.datarep, self.dsname)

    def rows(self):
        """
        Return/yield tuples or lists corresponding to each row to be inserted.
        """
        with self.input().open('r') as fobj:
            fobj.readline() # Skip the header line
            for line in fobj:
                yield line.strip('\n').split(',') # the separator is ','
