# coding: utf-8

"""Module dedicated write some Luigi tasks for analysis purposes
"""

import os.path as osp

import luigi
from luigi.format import UTF8

import pandas as pd

from extract_user_editor import editor_name, editor_count, get_top_editor


OUTPUT_DIR = 'output-extracts'


class TopMostUsedEditors(luigi.Task):
    """Compute the most used editor. Transform the editor name such as JOSM/1.2.3
    into josm in order to have
    """
    datarep = luigi.Parameter("data")
    fname = 'most-used-editor'
    editor_fname = 'soft-used-by-users.csv'

    def output(self):
        return luigi.LocalTarget(
            osp.join(self.datarep, OUTPUT_DIR, self.fname + ".csv"),
            format=UTF8)

    def run(self):
        with open(osp.join(self.datarep, OUTPUT_DIR, self.editor_fname)) as fobj:
            user_editor = pd.read_csv(fobj)
        # extract the unique editor name aka fullname
        user_editor['fullname'] = user_editor['value'].apply(editor_name)
        editor = editor_count(user_editor)
        top_editor = get_top_editor(editor)
        with self.output().open('w') as fobj:
            top_editor.to_csv(fobj, index=False)
