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
    - opened_at: change set opening date (seen as the firt modification timestamp)
    - user_lastchgset: difference between current change set opening date and user previous change set opening date 
    - lastmodif_at: change set closing date (seen as the last modification timestamp)
    - duration_insec: change set duration, in seconds (seen as the temporal difference between first and last modifications)
    - n_<elem/node/way/relation>_modif: number of <element/node/way/relation> modifications in the change set
    - n_<elem/node/way/relation>_modif_cr: number of <element/node/way/relation> creations in the change set
    - n_<elem/node/way/relation>_modif_del: number of <element/node/way/relation> improvements in the change set
    - n_<elem/node/way/relation>_modif_imp: number of <element/node/way/relation> deletions in the change set
    - n<min/med/max>_modif_byelem: <minimal/median/maximal> number of modifications per unique element
    - n_<elem/node/way/relation>: number of unique <element/node/way/relation> that have been modified in the change set
    - n_<elem/node/way/relation>_cr: number of unique <element/node/way/relation> that have been created in the change set
    - n_<elem/node/way/relation>_imp: number of unique <element/node/way/relation> that have been improved in the change set
    - n_<elem/node/way/relation>_del: number of unique <element/node/way/relation> that have been deleted in the change set
    
#### users

TO BE CONTINUED :warning: :warning: :warning:

This last metadata structure is the main metadata source. It focuses on OSM users (primary key: `{uid}`):

    - uid: contributor ID
    - n_chgset: number of change set opened by the contributor
    - first_at: date of the user first modification
    - last_at: data of the user last modification
    - activity: user activity on the API (seen as the difference between the first and last contributions)
    - t<min/med/max>_between_chgsets_inhour: <minimal/median/maximal> time between two change set creations (in hour)
    - d<min/med/max>_chgset_insec: <minimal/median/maximal> change set duration (in seconds) -- by definition <86400
    - n<min/med/max>_modif_bychgset: <minimal/median/maximal> number of modifications done by the user per change set
    - n<min/med/max>_elem_bychgset: <minimal/median/maximal> number of OSM elements modified by the user per change set
    - t<min/med/max>_update_inhour: <minimal/median/maximal> time of validity for user contributions (before the next version)
    - t<min/med/max>_corr_inhour: <minimal/median/maximal> time of validity for user contributions (before a modification by another user)
    - n_<elem/node/way/relation>_corr: number of <element/node/way/relation> that have been modified by the user and corrected after
    - t<min/med/max>_autocorr_inhour: <minimal/median/maximal> time of validity for user contributions (before a modification by the same user)
    - n_<elem/node/way/relation>_autocorr: number of <element/node/way/relation> that have been modified by the user and auto-corrected after
    - n_<elem/node/way/relation>_modif: number of <element/node/way/relation> modifications done by the user
    - n_<elem/node/way/relation>_modif_cr: number of <element/node/way/relation> creations done by the user
    - n_<elem/node/way/relation>_modif_crutd: number of <element/node/way/relation> creations done by the user and still valid at the extraction date
    - n_<elem/node/way/relation>_modif_crmod: number of <element/node/way/relation> creations done by the user and modified since
    - n_<elem/node/way/relation>_modif_crdel: number of <element/node/way/relation> creations done by the user and deleted since 
    - n_<elem/node/way/relation>_modif_del: number of <element/node/way/relation> deletions done by the user 
    - n_<elem/node/way/relation>_modif_delutd: number of <element/node/way/relation> deletions done by the user and still invisible at the extraction date 
    - n_<elem/node/way/relation>_modif_delrebirth: number of <element/node/way/relation> deletions by the user and reset since
    - v<min/med/max>_modif_del: <minimal/median/maximal> version of elements when deleted by the user
    - n_<elem/node/way/relation>_modif_imp: number of <element/node/way/relation> improvements done by the user
    - n_<elem/node/way/relation>_modif_imputd: number of <element/node/way/relation> improvements done by the user and still valid at the extraction date
    - n_<elem/node/way/relation>_modif_impmod: number of <element/node/way/relation> improvements done by the user and modified since
    - n_<elem/node/way/relation>_modif_impdel: number of <element/node/way/relation> improvements done by the user and deleted since
    - v<min/med/max>_modif_imp: <minimal/median/maximal> version of elements when improved by the user
    - n<min/med/max>_modif_byelem: <minimal/median/maximal> number of modifications done by the user per unique elements
    - n_<elem/node/way/relation>_with_1_contrib: number of <element/node/way/relation> with one single contribution done by the user
    - n_<elem/node/way/relation>: number of <element/node/way/relation> for which the user have contributed
    - n_<elem/node/way/relation>_cr: number of <element/node/way/relation> created by the user and still valid on the API
    - v<min/med/max>_cr: <minimal/median/maximal> version reached by elements created by the user
    - n_<elem/node/way/relation>_cr_wrong: number of <element/node/way/relation> created by the user and deleted later
    - v<min/med/max>_cr_wrong: <minimal/median/maximal> version reached by elements wrongly created by the user (i.e. elements dropped from the API)
    - n_<elem/node/way/relation>_imp: number of <element/node/way/relation> improved by the user and still valid on the API
    - v<min/med/max>_imp: <minimal/median/maximal> version reached by elements improved by the user
    - n_<elem/node/way/relation>_imp_wrong: number of <element/node/way/relation> improved by the user and deleted later
    - v<min/med/max>_imp_wrong: <minimal/median/maximal> version reached by elements improved by the user but dropped from the API since
    - n_<elem/node/way/relation>_del: number of <element/node/way/relation> deleted by the user and still invisible on the API
    - v<min/med/max>_del: <minimal/median/maximal> version reached by elements deleted by the user
    - n_<elem/node/way/relation>_del_wrong: number of <element/node/way/relation> deleted by the user and reset on the API later
    - v<min/med/max>_del_wrong: <minimal/median/maximal> version reached by elements wrongly deleted by the user (i.e. elements not dropped from the API)