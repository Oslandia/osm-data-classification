Projet du groupe interne Data / IA / ML / Stats

Starring with:
- dga
- rde
- hme
- oco

___

# Content of the project

Three repositories composed this project, namely *demo*, *refs* and *src*.

## *demo* repository
In this repository a R notebook describes some basic elements about OSM history data, by using a sample related to the city of Bordeaux. This notebook is in a pre-compiled version in a .html format. The analysis has been reproduced in Python (see src repository).

## *figs* repository
Here are a set of .png files that graphically describes some basic OSM metadata features (number of version by elements, number of modifications by change sets, number of change sets by users and so on...).

## *refs* repository
This repository contains some bibliographic references dedicated to OSM data quality. The files are named in the following format '<year>_<nameoffirstauthor>'. A mention is added at the end of the file if it is a Phd report.

By the way a bibtex file summarizes the bibliography entry, if needed in a further pulication effort.

## *src* repository
In this repository, we gather all source files used in the R&D effort, by the way of a bunch of Python scripts. These source files are as follow:
- *OSM-history-parsing.py*: it extracts OSM history data from a native OSM file (in .pbf format), by returning a .csv files that contains the history of every OSM elements (nodes, ways, relations).
- *node-history-stats.py*: same process than the previous source file, dedicated to OSM node history extraction.
- *way-history-stats.py*: same process than *OSM-history-parsing.py* dedicated to OSM way history extraction.
- *relation-history-stats.py*: same process than *OSM-history-parsing.py* dedicated to OSM relation history extraction.
- *OSM-metadata-extract.py*: from the OSM element history .csv files, it produces different .csv files aiming to describe OSM metadata (on element themselves, change sets and users).
- *OSM-metadata-plotting.py*: From OSM metadata in .csv format, the purpose of this file is to plot some basic metadata features.
- *OSM-validity-check.py*: considering that extractions with *pyosmium* do not set the 'visible' tag properly (whether or not an OSM element is visible on the web API), we have designed some hypothesis on the element visibility. This file provides a verification of the hypothesis validity, through http requesting on element samples (starting from <element>-history-stats.csv files).
- *OSM-latest-data.py*: this file is dedicated to the extraction of up-to-date OSM elements in .csv format, starting from the .csv-formatted OSM history. After running this module, we get one file per type (node, way, relation), describing how the OSM map was at the data extraction date.
- *utils.py*: This last file contains some meaningful functions, useful for other modules.