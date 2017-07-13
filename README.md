# Introduction

Projet du groupe interne Data / IA / ML / Stats

Starring with:
- dga
- rde
- hme

___

# Project description

## Content

Five repositories composed this project, namely *article*, *demo*, *figs*, *refs* and *src*.

### *article*
Here are some articles that summarize the project development, these articles are planned to be posted on the Oslandia blog.

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

    - `analysis_tasks.py`: contains main analysis Luigi tasks;
    - `data_preparation_tasks.py`: contains parsing Luigi tasks;
    - `extract-changesets.py`: take the `changesets-latest.osm` from
      http://planet.openstreetmap.org/planet/ and turn it into a huge CSV file (~210Go)
    - `extract_user_editor.py`: from a native OSM editor extraction, build the ranking of editors for each user
	- `metadata_plotting.py`: some plotting function designed for illustrating metadata
	- `osm_carroying.sql`: merging between OSM metadata with geometries and INSEE carroyed data (200-square-meter tiles)
    - `osmparsing.py`: OSM data parser classes, built as pyosmium handlers, these classes allow to extract OSM history data from a native OSM file (in .pbf format) and return .csv files;
    - `output_tasks.py`: main source files, containing every final Luigi tasks;
    - `process-changesets-user-history.py` : take the previous changesets
      history CSV file and extract the total number of changesets by user and
      the used OSM editors by user thanks to dask, i.e. to process large CSV file.
	- `psql_draft.sql`: a short sql script aiming at merging OSM object metadata with their corresponding geometries
    - `tagmetanalyse.py`: functions used in the context of tag genome analysis;
	- `unsupervised_learning.py`: some utilitary functions dedicated to unsupervised learning with OSM metadata
    - `utils.py`: some functions used all along the process by other modules;
	- `validitycheck.py`: test the OSM object visibility accuracy with some http requests

## How to run this code?

### Understanding the current Luigi framework

The script `osm-tasks.py` is the conductor in this project. It is composed of a set of Luigi tasks, defined as classes (this list is not exhaustive) :

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
    
## OSM metadata description

:warning: still to do !
