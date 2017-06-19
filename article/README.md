OSM data quality

---

# Export Org file to HTML

* In Emacs `M-x org-html-export-to-html`. It can run the Python block examples
  with a `ipython` kernel

* [From the command line](https://stackoverflow.com/questions/22072773/batch-export-of-org-mode-files-from-the-command-line) you can also do:

  `emacs article_filename.org --batch -f org-html-export-to-html --kill`

# Blog article draft

## Available elements

Main source: cf [dga article draft](https://git.oslandia.net/dgaraud/raster-vector-quality-data/blob/master/osm-history/article.org)

In addition to this work, we also have some mature contributions about metadata feature extractions, and a growing comprehension of user clustering.

### introduction

introduction written by dga = philosophy of OSM data extraction

how to go from .pdf format to .csv format


### spatial data quality

What are the characteristic of data quality?
***warning:*** build a strong bibliography


### Data retrieval

Several possibilities...

- temporal strategy:
  - extraction of an up-to-date picture of OSM data
  - extraction of OSM data history

- geographical strategy:
  - full-data extraction (complete, but heavy)
  - partial extraction, by country or region
  - through a bounding-box

### OSM data analysis

Start from .csv files (OSM elements, OSM users, OSM change sets, OSM tag genome)

Describe the results, develop simple statistics about OSM elements


### Luigi framework

A global workflow from the OSM history dumps to machine-learning analysis about OSM metadata



## Rebuild the blog article structure(s)

### First article

Present the global workflow: mentions to Luigi, description of each step of the process, and consequently, of each Luigi classes

One paragraph per step


### Second article

Introduction of the project, explain what is OSM data quality, call some references to make the speech stronger

Present OSM data, their format, the available features, write about the OSM API, with some examples

Develop the data extraction process (from .pbf to .csv)


### Third article

First analysis: how to extract OSM data history

historical evolution of a typical OSM area (*e.g.* =Bordeaux-metropole=)
- TODO: use datedelem function to extract pictures of OSM API at several dates (use Python time series capability)


### Fourth article

Second analysis: describe the set of tags applied to OSM elements, evaluate the tag key and value frequencies


### Fifth article

Third analysis: change set and user descriptions


### Sixth article

User metadata analysis with machine learning (pca + clustering)


### Seventh article

Use contributor classification to evaluate OSM entities (*e.g.* roads)

Build OSM maps using the previous classification

:warning: ***still to do***


### Eighth article

Comparison between OSM quality maps and a "ground truth", given by alternative data sources (*e.g.* Bing orthoimages)

:warning: ***still to do***
