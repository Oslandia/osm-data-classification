# coding: utf-8

"""Module dedicated write some Luigi tasks for analysis purposes
"""

import os.path as osp

import luigi
from luigi.format import UTF8

import pandas as pd

from extract_user_editor import editor_name, editor_count, get_top_editor
from osm_task import UserMetadataExtract


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


class AddExtraInfoUserMetadata(luigi.Task):
    """Add extra info to User metadata such as used editor and total number of changesets
    """
    datarep = luigi.Parameter("data")
    dsname = luigi.Parameter()
    # take first 15th most used editors
    n_top_editor = 15
    fname = 'user-metadata-extra'
    editor_fname = 'soft-used-by-users.csv'
    total_user_changeset_fname = 'all-users-count-changesets.csv'

    def output(self):
        return luigi.LocalTarget(
            osp.join(self.datarep, OUTPUT_DIR, self.dsname, self.fname + ".csv"),
            format=UTF8)

    def requires(self):
        return {'top_editor': TopMostUsedEditors(self.datarep),
                'user_metadata': UserMetadataExtract(self.datarep, self.dsname)}

    def run(self):
        with self.input()['user_metadata'].open() as fobj:
            users = pd.read_csv(fobj, index_col=0)
        with self.input()['top_editor'].open() as fobj:
            top_editor = pd.read_csv(fobj)
            # print(top_editor)
        with open(osp.join(self.datarep, OUTPUT_DIR, self.editor_fname)) as fobj:
            user_editor = pd.read_csv(fobj)
        with open(osp.join(self.datarep, OUTPUT_DIR, self.total_user_changeset_fname)) as fobj:
            changeset_count_users = pd.read_csv(fobj, header=None,
                                                names=['uid', 'num'])
        # add total number of changesets to the 'users' DataFrame
        users = (users
                 .join(changeset_count_users.set_index('uid'), on='uid')
                 .rename_axis({'num': 'n_total_chgset'}, axis=1))
        # create `n_top_editor` cols
        selected_editor = (top_editor.sort_values(by="num", ascending=False)
                           .iloc[:self.n_top_editor]['fullname']
                           .values
                           .tolist())
        user_editor['name'] = user_editor['value'].apply(editor_name)
        set_label = lambda x: x if x in selected_editor else 'other'
        user_editor['label'] = user_editor['name'].apply(set_label)
        # number of times an editor is used by user
        used_editor_by_user = (user_editor.groupby(['uid', 'label'])['num']
                               .sum()
                               .unstack()
                               .fillna(0))
        # add `n_top_editor` columns with the number of time that each user used them
        users = users.join(used_editor_by_user, on='uid')
        with self.output().open('w') as fobj:
            users.to_csv(fobj, index=False)
