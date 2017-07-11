# coding: utf-8

"""List of classes aiming to extract information from a history OSM data
file. This information deals with OSM element history, or tag genome
history. Other extracts are possible, however we let these developments for
further investigations.

"""

import pandas as pd
import osmium as osm

#####

DEFAULT_START = pd.Timestamp("2000-01-01T00:00:00Z")

#####
wkb_factory = osm.geom.WKBFactory()

class GeometryHandler(osm.SimpleHandler):
    """
    """

    def __init__(self):
        osm.SimpleHandler.__init__(self)
        self.geometries = []
            
    def node(self, n):
        nloc = n.location
        if nloc.valid():
            self.geometries.append(["node",
                                    n.id,
                                    n.version,
                                    n.visible,
                                    wkb_factory.create_point(nloc)])
        else:
            self.geometries.append(["node",
                                    n.id,
                                    n.version,
                                    n.visible,
                                    ""])

    def way(self, w):
        validway = True
        for n in w.nodes:
            if not n.location.valid():
                validway = False
                break
        if validway and len(w.nodes) > 1:
            if not w.is_closed():
                self.geometries.append(["way",
                                        w.id,
                                        w.version,
                                        w.visible,
                                        wkb_factory.create_linestring(w.nodes)])
        else:
            self.geometries.append(["way", w.id, w.version, w.visible, ""])


class AreaHandler(osm.SimpleHandler):
    def __init__(self):
        osm.SimpleHandler.__init__(self)
        self.areas = []

    def area(self, a):
        self.areas.append([a.id,
                           a.from_way(),
                           a.is_multipolygon(),
                           a.orig_id(),
                           a.num_rings(),
                           a.version,
                           a.visible,
                           pd.Timestamp(a.timestamp),
                           a.uid,
                           a.changeset])
        
class TagGenomeHandler(osm.SimpleHandler):
    """Encapsulates the recovery of tag genome history

    This tag genome consists in each tag associated to OSM elements whenever
    the element has been integrated to OSM. In addition to element-focused
    features (elem type, id and version), two features are supposed to describe
    the tags (key and value).

    """
    
    def __init__(self):
        osm.SimpleHandler.__init__(self)
        self.taggenome = []

    def tag_inventory(self, elem, elem_type):
        for tag in elem.tags:
            self.taggenome.append([elem_type,
                                   elem.id,
                                   elem.version,
                                   tag.k,
                                   tag.v])

    def node(self, n):
        self.tag_inventory(n, "node")

    def way(self, w):
        self.tag_inventory(w, "way")

    def relation(self, r):
        self.tag_inventory(r, "relation")

#####
        
class TimelineHandler(osm.SimpleHandler):
    """Encapsulates the recovery of elements inside the OSM history.

    This history is composed of nodes, ways and relations, that have common
    attributes (id, version, visible, timestamp, userid, changesetid). Nodes
    are characterized with their latitude and longitude; ways with the nodes
    that composed them; relations with a list of OSM elements that compose it,
    aka their members (a member can be a node, a way or another relation). We
    gather these informations into a single attributes named 'descr'. The
    timeline handler consider the OSM element type as well (node, way or
    relation). OSM history dumps do not seem to update properly 'visible' flag
    of OSM elements, so this handler recode it according to elements property.

    """
    def __init__(self):
        """ Class default constructor"""
        osm.SimpleHandler.__init__(self)
        self.elemtimeline = [] # Dictionnary of OSM elements
        print("<TRACE> Initialization of a TimelineHandler instance !")
        
    def node(self,n):
        """Node recovery: each record in the history is saved as a row in the
        element dataframe.
        
        The features are the following: elem type ("node"), id, version,
        visible?, timestamp, userid, chgsetid

        """
        self.elemtimeline.append(["node",
                                  n.id,
                                  n.version,
                                  n.visible,
                                  pd.Timestamp(n.timestamp),
                                  n.uid,
                                  n.changeset])


    def way(self,w):
        """Way recovery: each record in the history is saved as a row in the
        element dataframe.

        The features are the following: elem type ("way"), id, version,
        visible?, timestamp, userid, chgsetid -- Note: a way that does not
        contain any node is considered as unavailable

        """
        self.elemtimeline.append(["way",
                                  w.id,
                                  w.version,
                                  w.visible,
                                  pd.Timestamp(w.timestamp),
                                  w.uid,
                                  w.changeset])

                                     
    def relation(self,r):
        """Relation recovery: each record in the history is saved as a row in
        the element dataframe.

        The features are the following: elem type ("relation"), id, version,
        visible?, timestamp, userid, chgsetid -- Note: a relation withouth any
        member is considered as unavailable

        """
        self.elemtimeline.append(["relation",
                                  r.id,
                                  r.version,
                                  r.visible,
                                  pd.Timestamp(r.timestamp),
                                  r.uid,
                                  r.changeset])
