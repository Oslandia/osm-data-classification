#!/usr/bin/env python3
# coding: utf-8

"""Extract changeset from the (huge) OSM history changeset XML file.
"""

import sys
import re


id_re = r'id="(?P<id>\d+)"'
created_re = r'created_at="(?P<created>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)"'
# closed_re = r'closed_at="(?P<closed>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)"'
userid_re = r'.*uid="(?P<uid>\d+)"'
float_re = r'[-+]?[0-9]*\.?[0-9]*.'
box_re = r'min_lat="(?P<min_lat>RE)" min_lon="(?P<min_lon>RE)" max_lat="(?P<max_lat>RE)" max_lon="(?P<max_lon>RE)"'.replace('RE', float_re)
num_re = r'num_changes="(\d+)"'
comments_re = r'comments_count="(\d+)"'
changeset_pattern = re.compile(r' '.join([r'\s+<changeset',
                                          id_re,
                                          created_re,
                                          # closed_re,
                                          userid_re,
                                          box_re,
                                          num_re,
                                          comments_re]))
# sometimes, there is not a bbox as attribute
changeset_wo_bbox_pattern = re.compile(r' '.join([r'\s+<changeset',
                                                  id_re,
                                                  created_re,
                                                  # closed_re,
                                                  userid_re,
                                                  num_re,
                                                  comments_re]))
tag_re = re.compile(r'  <tag k="(?P<key>.*)" v="(?P<value>.*)"/>')

def is_bbox(line):
    """is there a bbox as attribute of a changeset

    i.e. min_lat, max_lon, min_lon, max_lin
    """
    if 'min_lat' in line:
        return True
    return False


def main(fname, output_fname):
    with open(fname) as fobj, open(output_fname, 'w') as out:
        out.write("id,created,uid,min_lat,min_lon,max_lat,max_lon,num_changes,comments_count,key,value\n")
        tags = []
        multiple_lines = []
        # values = []
        for xmlline in fobj:
            if is_bbox(xmlline):
                match = changeset_pattern.match(xmlline)
                bbox = True
            else:
                match = changeset_wo_bbox_pattern.match(xmlline)
                bbox = False
            if match:
                data = list(match.groups())
                if not bbox:
                    # add empty value for min_lat, max_lat, etc.
                    data = data[:3] + [''] * 4 + data[3:]
                line = ",".join(data) + ","
                # There is not tags for this changeset
                if xmlline.endswith('/>\n'):
                    out.write(line + ',\n')
                    continue
                multiple_lines = []
                tags = []
            else:
                tagmatch = tag_re.match(xmlline)
                if tagmatch:
                     tags.append(tuple(tagmatch.groups()))
                # if </changeset>, you can write all tags
                # one line by tag. repeat the first line for each
                if '</changeset>' in xmlline:
                    for key, value in tags:
                        multiple_lines.append(line + '"' + key + '","' + value + '"')
                    out.write("\n".join(multiple_lines))
                    out.write("\n")


if len(sys.argv) != 3:
    print("ERROR: need an input file and output file")
    sys.exit(0)

INPUT_FNAME = sys.argv[1]
OUTPUT_FNAME = sys.argv[2]
main(INPUT_FNAME, OUTPUT_FNAME)
