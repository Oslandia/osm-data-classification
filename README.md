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
In this repository a R notebook describes some basic elements about OSM history data, by using a sample related to the city of Bordeaux. This notebook is in .Rmd format (it has to be opened with an R-prompt, or a dedicated tool like Rstudio), and in a pre-compiled version in a .html format.

## *refs* repository
This repository contains some bibliographic references dedicated to OSM data quality. The files are named in the following format '<year>_<nameoffirstauthor>'. A mention is added at the end of the file if it is a Phd report.

By the way a bibtex file summarizes the bibliography entry, if needed in a further pulication effort.

## *src* repository
In this repository, we gather all source files used in the R&D effort. First we have some python scripts aiming to extract data from a OSM history file (in the .pbf format). *node-history-stats.py*, *way-history-stats-py* and *relation-history-stats.py* give respectively .csv files where nodes, ways and relations are stored in.

A file *OSMdata.R* is also available; it is a draft used for the notebook *OSM_nb* preparation.