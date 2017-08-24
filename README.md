# OpenStreetMap Data Quality based on the Contributions History

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

## Dependencies

Works with Python 3

* pyosmium
* luigi
* pandas
* statsmodels
* scikit-learn
* matplotlib
* seaborn

There is a `requirements.txt` file. Thus, do `pip install -r requirements.txt`
from a virtual environment.

## How does it work?

Several Python files to extract and analyze the OSM history data. Two machine
learning models are used to classify the changesets and the OSM contributors.

* Dimension reduction with [PCA](https://en.wikipedia.org/wiki/Principal_component_analysis)
* Clustering with the [KMeans](https://en.wikipedia.org/wiki/K-means_clustering)

The purpose of the PCA **is not** to reduce the dimension (you have less than
100 features). It's to analyze the different features and understand the most
important ones.

## Running

### Get some history data

First, get some OSM history data.

You can get some history data for a specific world region
on [Geofabrik](http://download.geofabrik.de/). You have to download a
`*.osh.pbf` file. For instance, on
the [Greater London page](http://download.geofabrik.de/europe/great-britain/england/greater-london.html),
you can download the
file [greater-london.osh.pbf](http://download.geofabrik.de/europe/great-britain/england/greater-london.osh.pbf).

### Organize your output data directories

Create a `data` directory and some subdirs elsewhere. The data processing should
be launched from the folder where you have your `data` folder.

* `mkdir -p data/output-extracts`
* `mkdir data/raw`

Then, copy your fresh downloaded `*.osh.pbf` file into the `data/raw/`
directory.

**Note**: if you want another name for your data directory, you'll be able to
specify the name thanks to the `--datarep` luigi option.


### The limits of the data pipeline

The data pipeline processing is handle
by [luigi](http://luigi.readthedocs.io/). It's very useful because it can build
a dependencies graph of your different processing tasks (it's a DAG) and launch
them in parallel when it's possible.

These tasks yield output files (CSV, JSON, hdf5, png). Some files such as
`all-changesets-by-user.csv` and `all-editors-by-user.csv` needed for some tasks
was built outside of this pipeline. Actually, these files come from the big
`changesets-latest.osm` XML file which is difficult to include in the pipeline
because:

- the processing can be a quite long
- you should have a large amount of RAM

Thus, you can get these two CSV files in the `osm-user-data` and copy them into
your `data/output-extracts` directory.

See also "I want to parse the changesets.osm file!"

### Run your first analyze

You should have the following files:

```
data
data/raw
data/raw/specific-region.osh.pbf
data/output-extracts
data/output-extracts/all-changesets-by-user.csv
data/output-extracts/all-editors-by-user.csv
```

Launch

`luigi --local-scheduler --module output_tasks MasterTask --dsname specific-region`

`dsname` mean "dataset name". It must have the same name as your `*.osh.pbf`
file.

Most of the time (if you have an import Python error), you have to preprend the
luigi command by the `PYTHONPATH` environment variable to the
`osm-data-quality/src` directory. Such as:

`PYTHONPATH=/path/to/osm-data-quality/src luigi --local-scheduler ...`

The `MasterTask` chooses the number of PCA components and the number of KMeans
clusters in an automatic way. If you want to set the number of clusters for
instance, you can pass the following options to the luigi command:

`--module analysis_tasks KMeansFromPCA --dsname specific-region --n-components 6 --nb-clusters 5`

In this case, the PCA will be carried out with 6 components. The clustering will
use the PCA results to carry out the KMeans with 5 clusters.

See also the different luigi options in
the [official luigi documentation](http://luigi.readthedocs.io/en/stable/command_line.html).

## Results

You should have a `data/output-extracts/specific-region` directory with several
CSV, JSON and h5 files.

* Several intermediate CSV files.
* JSON KMeans report to see the "ideal" number of clusters (the key `n_clusters`).
* PCA hdf5 files with `/features` and `/individuals` keys.
* KMeans hdf5 files with `/centroids` and `/individuals` keys.
* A few PNG images.

Open the [results analysis notebook](./demo/results-analysis.ipynb) to see how
analyze the results.

## I want to parse the changesets.osm file

See http://planet.openstreetmap.org/planet/changesets-latest.osm.bz2

* Convert the file into a huge CSV file
* Group each user by editors and changesets thanks with [dask](https://github.com/dask/dask)

**TODO** : write the "how to"
