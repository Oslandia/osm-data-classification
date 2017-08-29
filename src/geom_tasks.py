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

    host = "localhost"
    database = "osm"
    user = "rde"
    password = ""
    table = luigi.Parameter("bordeaux_metropole_elements")
    columns = [("elem", "varchar"), ("id", "bigint"),
               ("first_at", "timestamp"), ("last_at", "timestamp"),
               ("lifespan", "float"), ("n_inscription_days", "float"),
               ("n_activity_days", "float"), ("version", "int"),
               ("n_chgset", "int"), ("n_user", "int"), ("n_autocorr", "int"),
               ("n_corr", "int"), ("visible", "boolean"), ("first_uid", "int"),
               ("last_uid", "int"), ("first_ug", "int"), ("last_ug", "int")]
    def requires(self):
        dataset_name = '-'.join(self.table.split('_')[:-1])
        return analysis_tasks.ElementMetadataExtract(self.datarep, dataset_name)

    def rows(self):
        """
        Return/yield tuples or lists corresponding to each row to be inserted.
        """
        with self.input().open('r') as fobj:
            header = fobj.readline() # Skip the header line
            column_names = header.strip('\n').split(',')
            cols = [column[0] for column in self.columns]
            col_indices = [column_names.index(c) for c in cols]
            for line in fobj:
                data = line.strip('\n').split(',') # the separator is ','
                yield [data[i] for i in col_indices]

class OSMElementGeometry(lpg.PostgresQuery):
    datarep = luigi.Parameter("data")

    host = "localhost"
    database = "osm"
    user = "rde"
    password = ""
    table = luigi.Parameter("bordeaux_metropole_geomelements")
    query = ""

    def requires(self):
        element_table_name = self.table.replace("geomelements", "elements")
        return OSMElementTableCopy(self.datarep, element_table_name)
        
    def run(self):
        dataset_name = "_".join(self.table.split("_")[:-1])
        connection = self.output().connect()
        cursor = connection.cursor()
        sql_geometry_merge = """
        SELECT l.osm_id, h.first_at, h.lifespan, h.n_activity_days,
        h.version, h.visible, h.first_ug, h.last_ug, h.n_user, h.n_chgset,
        h.n_autocorr, h.n_corr, l.way
        INTO {0}_geomelements
        FROM {0}_elements as h
        INNER JOIN {0}_line as l
        ON h.id = l.osm_id
        WHERE l.highway IS NOT NULL AND h.elem = 'way'
        ORDER BY l.osm_id;
        """.format(dataset_name)
        cursor.execute(sql_geometry_merge)
        self.output().touch(connection)
        connection.commit()
        connection.close()

class OSMElementGeomIndexCreation(lpg.PostgresQuery):
    datarep = luigi.Parameter("data")

    host = "localhost"
    database = "osm"
    user = "rde"
    password = ""
    table = luigi.Parameter("bordeaux_metropole_geomelements")
    query = ""

    def requires(self):
        return OSMElementGeometry(self.datarep, self.table)
    
    def run(self):
        dataset_name = "_".join(self.table.split("_")[:-1])
        connection = self.output().connect()
        cursor = connection.cursor()
        sql_index_creation = """
        CREATE INDEX {0}_geom_gist
        ON {0}_geomelements USING GIST(way);
        """.format(dataset_name)
        cursor.execute(sql_index_creation)
        self.output().touch(connection)
        connection.commit()
        connection.close()

