# Introduction

Projet du groupe interne Data / IA / ML / Stats

Starring with:
- dga
- rde
- hme
- oco

___

# Project description

## Content

Three repositories composed this project, namely *demo*, *refs* and *src*.

### *demo*
In this repository, notebooks are designed to present some snippets of OSM data analysis, in internal and external communication purposes.

### *figs*
Here are a set of .png files that graphically describes some basic OSM metadata features (number of version by elements, number of modifications by change sets, number of change sets by users and so on...).

### *refs*
This repository contains some bibliographic references dedicated to OSM data quality. The files are named in the following format '<year>_<nameoffirstauthor>'. A mention is added at the end of the file if it is a Phd report.

By the way a bibtex file summarizes the bibliography entry, if needed in a further pulication effort.

### *src*
In this repository, we gather all source files used in the R&D effort, by the way of a bunch of Python scripts. The project sources follow a [Luigi framework](https://luigi.readthedocs.io/en/stable/).

The source files are organized as follow:

    - `osm-tasks.py`: main source files, containing every Luigi tasks;
    - `utils.py`: some functions used all along the process by other modules;
    - `osmparsing.py`: OSM data parser classes, built as pyosmium handlers, these classes allow to extract OSM history data from a native OSM file (in .pbf format) and return .csv files;
    - `tagmetanalyse.py`: functions used in the context of tag genome analysis;
    - `count-changeset-by-user.py`: ?

## How to run this code?

### Understanding the current Luigi framework

The script `osm-tasks.py` is the conductor in this project. It is composed of a set of Luigi tasks, defined as classes :

    - `OSMHistoryParsing`: a parsing task, useful to extract the OSM data history, *i.e.* every records for each existing OSM entity (node, way and relation);
    - `OSMTagParsing`: another parsing task dedicated to the tag extraction, which returns the tag genome (one tag per row);
    - `OSMTagMetaAnalysis`: starting from the parsed tag genome, it evaluates the frequency of each tag key and each tag value for given tag key (TO BE CONTINUED) -- **final task** ;
    - `ElementMetadataExtract`: extract the metadata associated to each OSM element (node, way or relation), from the parsed OSM history -- **final task**;
    - `ChangeSetMetadataExtract`: extract the metadata associated to each OSM change set;
    - `OSMElementEnrichment`:
    - `UserMetadataExtract`: extract the metadata associated to each OSM contributor -- **final task**
    - `MasterTask`: a downstream task that aggregates all final tasks

These classes depends on two parameters (namely: `datarep` and `dsname`), that are the relative path to data directory on the user machine and the data set name (*e.g. bordeaux-metropole, aquitaine, france* and so on...).

### Run it from the command line!

Here are some example of command line utilization:

```bash
python -m luigi osm-tasks MasterTask --datarep datapath --dsname bordeaux-metropole --local-scheduler
python3 -m luigi osm-tasks UserMetadataExtract --local-scheduler
luigi --module osm-tasks OSMTagMetaAnalysis --dsname aquitaine-2017-03-23 --local-scheduler
```

It is possible to see in this examples that both parameters admit default values (they are not always explicitly set): `datarep=data` (a directory or a symbolic link to a directory in the source repository) and `dsname=bordeaux-metropole` (a small data set centered on the city of Bordeaux, France).

For further details about running Luigi command, please refer to the [Luigi documentation](https://luigi.readthedocs.io/en/stable/)

## OSM data description

### OSM history

The first dataframe gather every element modifications (primary key: `{elem,id,version}`):

    - elem: OSM entity type ("node", "way" or "relation")
    - id: ID of the element
    - version: version number, 1 if new element (remark: a few elements begin with a version>1)
    - visible: True or False, True if the element is visible on the OSM API, in the current version
    - ts: timestamp of the element modification
    - uid: ID of the user who did the modification
    - chgset: ID of the change set in which the modification took place
    - ntags: number of tags associated to the element, in the current version
    - tagkeys: keys of the element tags, in the current version

After running the task `OSMElementEnrichment`, this dataframe is brighted up with new features:

    - first_uid: ID of the user who created the element
    - vmax: up-to-date element version number
    - last_uid: ID of the last contributor
    - available: True if the element is visible in its up-to-date version, False otherwise
    - init: True if the version is the first known version of the element, False otherwise
    - up-to-date: True if the version is the last known version of the element, False otherwise
    - willbe_corr: True if a different user has proposed a more recent version for the current element, False otherwise
    - willbe_autocorr: True if the same user proposed a more recent version of the current element, False otherwise
    - nextmodif_in: time before the next element modification
    - nextcorr_in: time before the next element correction by a different user
    - nextauto_in: time before the next element correction by the same user
    
### OSM metadata

#### elements

This dataframe represents the first metadata structure, centered on OSM elements (primary key: `{elem,id}`):

    - elem: OSM entity type ("node", "way" or "relation")
    - id: ID of the element
    - version: total number of known versions
    - visible: True or False, True if the element is visible on the OSM API, in the last known version
    - n_user: number of unique contributors
    - n_chgset: number of change sets during which the element has been built
    - created_at: element creation timestamp
    - latsmodif_at: element last modification timestamp
    - lifecycle: difference between first and last modifications if the element is not visible any more, or between the first modification and the extraction date otherwise
    - mntime_between_modif: mean time between two modifications of the current element (equals to lifecycle/version)

#### chgsets

This second metadata structure focuses on OSM change sets (primary key: `{chgset}`):

    - chgset: ID of the current change set
    - uid: ID of user who created the change set
    - n_modif: number of modifications realized in the change set
    - n_nodemodif: number of node modifications realized in the change set
    - n_waymodif: number of way modifications realized in the change set
    - n_relationmodif: number of relation modifications realized in the change set
    - n_uniqelem: number of unique elements that have been modified in the change set
    - n_uniqnode: number of unique nodes that have been modified in the change set
    - n_uniqway: number of unique ways that have been modified in the change set
    - n_uniqrelation: number of unique relations that have been modified in the change set
    - opened_at: change set opening date (seen as the firt modification timestamp)
    - user_lastchgset: difference between current change set opening date and user previous change set opening date 
    - lastmodif_at: change set closing date (seen as the last modification timestamp)
    - duration_insec: change set duration, in seconds (seen as the temporal difference between first and last modifications)
    
#### users

TO BE CONTINUED :warning: :warning: :warning:

This last metadata structure is the main metadata source. It focuses on OSM users (primary key: `{uid}`):

    - uid : ID de l'utilisateur
    - n_chgset : nombre de change sets ouverts par l'utilisateur
    - n_modif_bychgset : nombre médian de modifications réalisées dans un change set
    - nmax_modif_bychgset : nombre maximum de modifications réalisées sur dans un seul change set
    - n_elem_bychgset : nombre médian d'éléments uniques modifiés dans un change set
    - nmax_elem_bychgset : nombre maximum d'éléments uniques modifiés dans un seul change set
    - meantime_between_chgset : différence temporelle moyenne entre deux créations de change set
    - n_modif : nombre de modifications réalisées par l'utilisateur
    - n_nodemodif : nombre de modifications sur les nodes réalisées par l'utilisateur
    - n_waymodif : nombre de modifications sur les ways réalisées par l'utilisateur
    - n_relationmodif : nombre de modifications sur les relations réalisées par l'utilisateur
    - nmed_modif_byelem : nombre médian de modifications réalisées sur un seul élément
    - nmed_modif_bynode : nombre médian de modifications réalisées sur un seul node
    - nmed_modif_byway : nombre médian de modifications réalisées sur un seul way
    - nmed_modif_byrelation : nombre médian de modifications réalisées sur une seule relation
    - nmax_modif_byelem : nombre maximum de modifications réalisées sur un seul élément
    - nmax_modif_bynode : nombre maximum de modifications réalisées sur un seul node
    - nmax_modif_byway : nombre maximum de modifications réalisées sur un seul way
    - nmax_modif_byrelation : nombre maximum de modifications réalisées sur une seule relation
    - first_at : date de la première modification réalisée par l'utilisateur
    - last_at : date dela dernière modification réalisée par l'utilisateur
    - activity : durée d'activité de l'utilisateur sur Openstreetmap
    - update_medtime : durée médiane pendant laquelle une modification réalisée par l'utilisateur reste valide
    - corr_medtime : durée médiane pendant laquelle une modification reste valide avant d'être amendée par un autre utilisateur
    - autocorr_medtime : durée médiane pendant laquelle une modification reste valide avant d'être amendée par l'utilisateur
    - n_elem : nombre d'éléments auxquels l'utilisateur a contribué
    - n_node : nombre de noeuds auxquels l'utilisateur a contribué
    - n_way : nombre de ways auxquels l'utilisateur a contribué
    - n_relation : nombre de relations auxquelles l'utilisateur a contribué
    - n_elem_lastdel : nombre d'éléments supprimés par l'utilisateur (à mettre en relation avec n_elem)
    - n_node_lastdel : nombre de noeuds supprimés par l'utilisateur (à mettre en relation avec n_node)
    - n_way_lastdel : nombre de ways supprimés par l'utilisateur (à mettre en relation avec n_way)
    - n_rel_lastdel : nombre de relations supprimées par l'utilisateur (à mettre en relation avec n_relation)
    - n_elem_lastupd : nombre d'éléments pour lequel l'utilisateur est responsable de la dernière mise à jour (à mettre en relation avec n_elem)
    - n_node_lastupd : nombre de nodes pour lequel l'utilisateur est responsable de la dernière mise à jour (à mettre en relation avec n_node)
    - n_way_lastupd : nombre de ways pour lequel l'utilisateur est responsable de la dernière mise à jour (à mettre en relation avec n_way)
    - n_rel_lastupd : nombre de relations pour lequel l'utilisateur est responsable de la dernière mise à jour (à mettre en relation avec n_relation)
    - n_elem_olddel : nombre d'éléments supprimés par un autre utilisateur après que l'utilisateur y ait contribué (à mettre en relation avec n_elem)
    - n_node_olddel : nombre de noeuds supprimés par un autre utilisateur après que l'utilisateur y ait contribué (à mettre en relation avec n_node)
    - n_way_olddel : nombre de ways supprimés par un autre utilisateur après que l'utilisateur y ait contribué (à mettre en relation avec n_way)
    - n_rel_olddel : nombre de relations supprimées par un autre utilisateur après que l'utilisateur y ait contribué (à mettre en relation avec n_relation)
    - n_elem_oldupd : nombre d'éléments auxquels l'utilisateur a contribué et qui ont été mis à jour depuis par un autre utilisateur (à mettre en relation avec n_elem)
    - n_node_oldupd : nombre de nodes auxquels l'utilisateur a contribué et qui ont été mis à jour depuis par un autre utilisateur (à mettre en relation avec n_node)
    - n_way_oldupd : nombre de ways auxquels l'utilisateur a contribué et qui ont été mis à jour depuis par un autre utilisateur (à mettre en relation avec n_way)
    - n_rel_oldupd : nombre de relations auxquelles l'utilisateur a contribué et qui ont été mises à jour depuis par un autre utilisateur (à mettre en relation avec n_relation)
    - n_elem_cr : nombre d'éléments créés par l'utilisateur
    - n_node_cr : nombre de nodes créés par l'utilisateur
    - n_way_cr : nombre de ways créés par l'utilisateur
    - n_relation_cr : nombre de relations créées par l'utilisateur
    - n_elem_crupd : nombre d'éléments créés par l'utilisateur et toujours dans leur version initiale
    - n_node_crupd : nombre de nodes créés par l'utilisateur et toujours dans leur version initiale
    - n_way_crupd : nombre de ways créés par l'utilisateur et toujours dans leur version initiale
    - n_relation_crupd : nombre de relations créées par l'utilisateur et toujours dans leur version initiale
    - n_elem_crmod : nombre d'éléments créés par l'utilisateur et modifiés depuis leur création
    - n_node_crmod : nombre de nodes créés par l'utilisateur et modifiés depuis leur création
    - n_way_crmod : nombre ways créés par l'utilisateur et modifiés depuis leur création
    - n_relation_crmod : nombre de relations créées par l'utilisateur et modifiées depuis leur création
    - n_elem_crdel : nombre d'éléments créés par l'utilisateur et supprimés depuis leur création
    - n_node_crdel : nombre de nodes créés par l'utilisateur et supprimés depuis leur création
    - n_way_crdel : nombre de ways créés par l'utilisateur et supprimés depuis leur création
    - n_relation_crdel : nombre de relations créées par l'utilisateur et supprimées depuis leur création
    