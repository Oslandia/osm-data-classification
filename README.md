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

# OSM data description

## OSM history

    -elem : type de l'entité OSM ("node", "way", "relation")
    - id: ID de l'entité OSM
    - version : numéro de version de l'élément (1 si nouvel élément)
    - visible : True ou False, True si l'élément <id> est visible sur l'API OSM dans sa version <version>
    - ts : date de la mise à jour de l'élément dans sa version courante
    - uid : ID de l'utilisateur ayant procédé à la modification
    - chgset : ID du change set dans lequel a été réalisé la modification
    - ntags : nombre de tags associés à l'élément dans sa version courante
    - tagkeys : clés des tags associés à l'élément dans sa version courange
    - descr : (lat, lon) pour les nodes, <liste des nodes> pour les ways, <liste des membres> pour les relations (ndla: un membre est caractérisé par la structure (id, type, role), où id est l'ID de l'élément OSM, type son type, ie "node", "way" ou "relation" et role le rôle de l'élément au sein de la structure, par exemple "inner", "outer", ...)

## OSM metadata

### elements

    - elem : type ("node", "way", "relation")
    - id : ID de l'élément
    - version : numéro de version de l'élément (1 si nouvel élément)
    - visible : True ou False, en fonction de la disponibilité de l'élément dans l'API d'OSM
    - n_user : nombre de contributeurs uniques 
    - n_chgset : nombre de change sets nécessaires à la construction de l'élément
    - created_at : date de création de l'élément
    - lastmodif_at : date de dernière modification
    - lifecycle : différence entre première et dernière modification si l'élément n'est plus visible, ou entre première modification et date d'extraction des données sinon
    - mntime_between_modif : durée moyenne entre deux modifications sur l'élément courant (équivalent à lifecycle/version)

### chgsets

    - chgset: ID du change set
    - uid : ID de l'utilisateur ayant créé le change set
    - n_modif : nombre de modifications dans le change set
    - n_nodemodif : nombre de modifications sur les nodes dans le change set
    - n_waymodif : nombre de modifications sur les ways dans le change set
    - n_relationmodif : nombre de modifications sur les relations dans le change set
    - n_uniqelem : nombre d'éléments modifiés dans le change set
    - n_uniqnode : nombre de nodes modifiés dans le change set
    - n_uniqway : nombre de ways modifiés dans le change set
    - n_uniqrelation : nombre de relations modifiées dans le change set
    - opened_at : date d'ouverture du change set (approximée par la date de première modification en son sein)
    - user_lastchgset : différence temporelle entre la date d'ouverture du change set et la précédente ouverture de change set par le même utilisateur
    - lastmodif_at : date de fermeture du change set (approximée par la date de la dernière modification)
    - duration : durée du change set (vue comme la différence temporelle entre la première et la dernière modification)
    
### users

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
    - n_modif_byelem : nombre médian de modifications réalisées sur un seul élément
    - n_modif_bynode : nombre médian de modifications réalisées sur un seul node
    - n_modif_byway : nombre médian de modifications réalisées sur un seul way
    - n_modif_byrelation : nombre médian de modifications réalisées sur une seule relation
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
    