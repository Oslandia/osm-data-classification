# coding: utf-8

""" Luigi implementation for OSM data analysis: in-base operations for parsing
OSM metadata and their geometry
"""

import luigi
import luigi.contrib.postgres as lpg
from luigi.format import MixedUnicodeBytes

import analysis_tasks

class OSMTableCreation(lpg.PostgresQuery):
    """ Luigi task:
    """
    host = luigi.Parameter("localhost")
    database = luigi.Parameter("osm")
    user = luigi.Parameter("rde")
    password = luigi.Parameter("")
    table = luigi.Parameter("bordeaux_metropole_elements")
    query = luigi.Parameter("""
        DROP TABLE IF EXISTS {0};
        CREATE TABLE {0}(
               elem varchar,
               id bigint,
               first_at timestamp,
               last_at timestamp,
               lifespan float,
               n_activity_days float,
               version int,
               n_chgset int,
               n_user int,
               n_autocorr int,
               n_corr int,
               visible boolean,
               first_uid int,
               last_uid int,
               first_ug int,
               last_ug int
        );
        """.format(table.task_value("", "")))
