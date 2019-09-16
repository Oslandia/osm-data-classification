# OpenStreetMap Data Quality based on the Contributions History

 [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Working with community-built data as [OpenStreetMap](https://openstreetmap.org)
forces to take care of data quality. We have to be confident with the data we
work with. Is this road geometry accurate enough? Is this street name missing?

Our first idea was to answer to this question: can we assess the **quality of
OpenStreetMap** data? (and how?).

This project is dedicated to **explore** and **analyze** the OpenStreetMap data
history in order to classify the contributors.

There are a serie of articles on
the [Oslandia's blog site](http://oslandia.com/en/category/data/) which deal
with this topic. Theses articles are also in the `articles` folder.

## How to install

This projects runs with Python3, every dependencies are managed
through [poetry](https://poetry.eustace.io/).

### Installation from source

```
$ git clone git@github.com:Oslandia/osm-data-classification.git
$ cd osm-data-classification
$ virtualenv -p /usr/bin/python3 venv
$ source venv/bin/activate
(venv)$ pip install poetry
(venv)$ poetry install
```

## How does it work?

There are several Python files to extract and analyze the OSM history data. Two
machine learning models are used to classify the changesets and the OSM
contributors.

* Dimension reduction with [PCA](https://en.wikipedia.org/wiki/Principal_component_analysis)
* Clustering with the [KMeans](https://en.wikipedia.org/wiki/K-means_clustering)

The purpose of the PCA **is not** to reduce the dimension (you have less than
100 features). It's to analyze the different features and understand the most
important ones.

## Running

### Get some history data

You can get some history data for a specific world region
on [Geofabrik](http://download.geofabrik.de/). You have to download a
`*.osh.pbf` file. For instance, on
the [Greater London page](http://download.geofabrik.de/europe/great-britain/england/greater-london.html),
you can download the
file [greater-london.osh.pbf](http://download.geofabrik.de/europe/great-britain/england/greater-london.osh.pbf).

**Warning:** Since GDPR, Geofabrik has modified its API. You have to be logged
in to the website with your OSM contributor account to download `osh.pbf` files, as OSM history files contain some private informations about OSM contributors.

### Organize your output data directories

Create a `data` directory and some subdirs elsewhere. The data processing should
be launched from the folder where you have your `data` folder (or alternatively, where a symbolic link points out to it).

* `mkdir -p data/output-extracts`
* `mkdir data/raw`

Then, copy your fresh downloaded `*.osh.pbf` file into the `data/raw/`
directory.

**Note**: if you want another name for your data directory, you'll be able to
specify the name thanks to the `--datarep` luigi option.

### The limits of the data pipeline

The data pipeline processing is handled
by [Luigi](http://luigi.readthedocs.io/), which can build a direct acyclic
dependency graph of your different processing tasks and launch them in parallel
when it's possible.

These tasks yield output files (CSV, JSON, hdf5, png). Some files such as
`all-changesets-by-user.csv` and `all-editors-by-user.csv` needed for some tasks
was built outside of this pipeline. Actually, these files come from the big
`changesets-latest.osm` XML file which is difficult to include in the pipeline
because:

- the processing can be a quite long
- you should have a large amount of RAM

Thus, you can get these two CSV files in the `user-data` folder and copy them
into your `data/output-extracts` directory (latest date of download: 2019-09).

See also the *I want to parse the changesets.osm file* section.

### Run your first analyze

You should have the following files:

```
data
data/raw
data/raw/region.osh.pbf
data/output-extracts
data/output-extracts/all-changesets-by-user.csv
data/output-extracts/all-editors-by-user.csv
```

In the virtual environment, launch:

`luigi --local-scheduler --module analysis_tasks AutoKMeans --dsname region`

or

`python3 -m luigi --local-scheduler --module analysis_tasks AutoKMeans --dsname region`

`dsname` mean "dataset name". It must have the same name as your `*.osh.pbf`
file.

*Note:* The default value of this parameter is `bordeaux-metropole`. If you do not set another value and if you do not have such `.osh.pbf` file onto your file system, the program will crash.

Most of the time (if you have an Python import error), you have to prepend the
luigi command by the `PYTHONPATH` environment variable to the
`osm-data-quality/src` directory. Such as:

`PYTHONPATH=/path/to/osm-data-quality/src luigi --local-scheduler ...`

The `MasterTask` chooses the number of PCA components and the number of KMeans
clusters in an automatic way. If you want to set the number of clusters for
instance, you can pass the following options to the luigi command:

`--module analysis_tasks KMeansFromPCA --dsname region --n-components 6 --nb-clusters 5`

In this case, the PCA will be carried out with 6 components. The clustering will
use the PCA results to carry out the KMeans with 5 clusters.

See also the different luigi options in
the
[official luigi documentation](http://luigi.readthedocs.io/en/stable/command_line.html).

## Results

You should have a `data/output-extracts/<region>` directory with several
CSV, JSON and h5 files.

* Several intermediate CSV files;
* JSON KMeans report to see the "ideal" number of clusters (the key `n_clusters`);
* PCA hdf5 files with `/features` and `/individuals` keys;
* KMeans hdf5 files with `/centroids` and `/individuals` keys;
* A few PNG images.

Open the [results analysis notebook](./demo/results-analysis.ipynb) to have an insight about how to exploit the results.

## I want to parse the changesets.osm file

See http://planet.openstreetmap.org/planet/changesets-latest.osm.bz2 (up-to-date changeset data).

1. Download the latest changesets files `changesets-latest.osm.bz2`
2. `bunzip2 changesets-latest.osm.bz2` to decompress the file. It can be a quite long.

This file is a XML file (>30Gb) which looks like

```xml
<?xml version="1.0" encoding="UTF-8"?>
<osm license="http://opendatacommons.org/licenses/odbl/1-0/" copyright="OpenStreetMap and contributors" version="0.6" generator="planet-dump-ng 1.1.6" attribution="http://www.openstreetmap.org/copyright" timestamp="2019-09-08T23:59:49Z">
 <bound box="-90,-180,90,180" origin="http://www.openstreetmap.org/api/0.6"/>
 <changeset id="1" created_at="2005-04-09T19:54:13Z" closed_at="2005-04-09T20:54:39Z" open="false" user="Steve" uid="1" min_lat="51.5288506" min_lon="-0.1465242" max_lat="51.5288620" max_lon="-0.1464925" num_changes="2" comments_count="11"/>
 <changeset id="2" created_at="2005-04-17T14:45:48Z" closed_at="2005-04-17T15:51:14Z" open="false" user="nickw" uid="94" min_lat="51.0025063" min_lon="-1.0052705" max_lat="51.0047760" max_lon="-0.9943439" num_changes="11" comments_count="2"/>
 <changeset id="3" created_at="2005-04-17T19:32:55Z" closed_at="2005-04-17T20:33:51Z" open="false" user="nickw" uid="94" min_lat="51.5326805" min_lon="-0.1566335" max_lat="51.5333176" max_lon="-0.1541054" num_changes="7" comments_count="0"/>
 <changeset id="4" created_at="2005-04-18T15:12:25Z" closed_at="2005-04-18T16:12:45Z" open="false" user="sxpert" uid="143" min_lat="51.5248871" min_lon="-0.1485492" max_lat="51.5289383" max_lon="-0.1413791" num_changes="5" comments_count="0"/>
 <changeset id="5" created_at="2005-04-19T22:06:51Z" closed_at="2005-04-19T23:10:02Z" open="false" user="nickw" uid="94" min_lat="51.5266800" min_lon="-0.1418076" max_lat="51.5291901" max_lon="-0.1411505" num_changes="3" comments_count="0"/>

...

 <changeset id="74238743" created_at="2019-09-08T23:59:21Z" closed_at="2019-09-08T23:59:23Z" open="false" user="felipeedwards" uid="337684" min_lat="-34.6160090" min_lon="-55.8347627" max_lat="-34.5975123" max_lon="-55.8167882" num_changes="10" comments_count="0">
  <tag k="import" v="yes"/>
  <tag k="source" v="Uruguay AGESIC 2018"/>
  <tag k="comment" v="Importación de datos de direcciones AGESIC 2019 #Kaart-TM-351 Ruta 11 José Batlle y Ordóñez"/>
  <tag k="hashtags" v="#Kaart-TM-351"/>
  <tag k="created_by" v="JOSM/1.5 (15238 es)"/>
 </changeset>
 <changeset id="74238744" created_at="2019-09-08T23:59:49Z" open="true" user="kz4" uid="8587542" min_lat="37.1581344" min_lon="29.6576262" max_lat="37.1690847" max_lon="29.6774139" num_changes="6" comments_count="0">
  <tag k="host" v="https://www.openstreetmap.org/edit"/>
  <tag k="locale" v="en-US"/>
  <tag k="comment" v="Added speed limit"/>
  <tag k="created_by" v="iD 2.15.5"/>
  <tag k="imagery_used" v="Maxar Premium Imagery (Beta)"/>
  <tag k="changesets_count" v="9150"/>
 </changeset>
</osm>
```

You must have the user id `uid` for each changeset. Most of the time, you'll have the
`created_by` key with the name of the editor, e.g. iD, JOSM, etc. with its version.

3. Run the script `extract-changesets.py` to turn the XML data into a CSV file
   (>32Gb), e.g.

   ```
   > path/to/osmdq/extract-changesets.py changesets-latest.osm changesets-latest.csv
   ```

   There will be one line by key/value pair for each changeset.


      id | created | uid | min_lat | min_lon | max_lat | max_lon | num_changes | comments | key | value
   --------|--------|--------|--------|--------|--------|--------|--------|--------|--------|--------
   53344191 | 2017-10-29T15:03:03Z | 2130431 | 60.6909827 | 16.2826338 | 60.8337425 | 16.3889430 | 600 | 0 | "comment" | "Added roads and lakes from Bing"
   53344191 | 2017-10-29T15:03:03Z | 2130431 | 60.6909827 | 16.2826338 | 60.8337425 | 16.3889430 | 600 | 0 | "created_by" | "iD 2.4.3"
   55673783 | 2018-01-23T05:50:50Z | 6401144 | 53.4504430 | 49.5804026 | 53.4504430 | 49.5804026 | 1 | 0 | "comment" | "Edit Resort."
   55673783 | 2018-01-23T05:50:50Z | 6401144 | 53.4504430 | 49.5804026 | 53.4504430 | 49.5804026 | 1 | 0 | "created_by" | "OsmAnd+ 2.8.2"
   55673784 | 2018-01-23T05:51:10Z | 6892267 | 9.9459612 | 8.8879152 | 9.9469054 | 8.8917745 | 1 | 0 | "source" | "Bing"
   55673784 | 2018-01-23T05:51:10Z | 6892267 | 9.9459612 | 8.8879152 | 9.9469054 | 8.8917745 | 1 | 0 | "comment" | "changed classification from tertiary to residential"
   55673784 | 2018-01-23T05:51:10Z | 6892267 | 9.9459612 | 8.8879152 | 9.9469054 | 8.8917745 | 1 | 0 | "created_by" | "JOSM/1.5 (13053 en)"

   If you read this file with pandas, you should have at least +50Gb RAM. But you can
   use [dask](https://github.com/dask/dask) which allows you to process in parallel the data
   without loading all the data.

4. To group each user by editor and changeset thanks to
   [dask](https://github.com/dask/dask), run the script `process-changesets-user-history.py`

   ```
   > python path/to/osmdq/process-changesets-user-history.py -i changesets-latest.csv -o all-editors-by-user.csv editor
   > python path/to/osmdq/process-changesets-user-history.py -i changesets-latest.csv -o all-changesets-by-user.csv changeset
   ```

   We don't add dask as a dependency of this project for this few Python scripts. If
   you want to run the 'process' script, you can install dask, cloudpickle, toolz and
   ffspec packages in a dedicated virtualenv. If the script does not fit your memory,
   check the `blocksize` and `num_workers` arguments of the `dd.read_csv` and
   `dask.config.set` functions respectively and adjust them.

5. The `all-editors-by-user.csv` file contains information about the user favorite
   editors (and their associated versions). You can transform these data to another
   CSV where you have one column by editor (without the version).

   ```
   > python path/to/osmdq/extract_user_editor.csv all-editors-by-user.csv editors-count-by-user.csv
   ```

## Who uses this project?

* A thesis about [Quality Assessment of Volunteered Geographic Information: An Investigation into the Ottawa-Gatineau OpenStreetMap Database](https://curve.carleton.ca/fb66a114-871d-4cac-bfb1-092a65a28ccc) by [@ktjaco](https://github.com/ktjaco)
