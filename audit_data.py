#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Your task is to use the iterative parsing to process the map file and
find out not only what tags are there, but also how many, to get the
feeling on how much of which data you can expect to have in the map.
Fill out the count_tags function. It should return a dictionary with the 
tag name as the key and number of times this tag can be encountered in 
the map as value.

Note that your code will be tested with a different data file than the 'example.osm'
"""
import xml.etree.cElementTree as ET
from collections import defaultdict
import re
import pprint
import codecs
import json

OSMFILE = "example.osm"
street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)

expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road",
            "Trail", "Parkway", "Commons","Turnpike","Terrace","Promenade","Plaza","Path","Circle","Way", "Loop"]

lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
addrStart = re.compile(r'^addr:.*$')
doubleColon = re.compile(r'.*:.*:.*')

CREATED = ["version", "changeset", "timestamp", "user", "uid"]

# UPDATE THIS VARIABLE
mapping = {"St": "Street",
           "St.": "Street",
           "st":"Street",
           "STREET":"Street",
           "street":"Street",
           "Steet":"Street",
           "Streeet":"Street",
           "Ave": "Avenue",
           "Ave.": "Avenue",
           "ave":"Avenue",
           "avenue":"Avenue",
           "AVENUE.": "Avenue",
           "Rd": "Road",
           "Rd.": "Road",
           "ROAD":"Road",
           "road":"Road",
           "Tpke":"Turnpike",
           "Trce":"Terrace",
           "Ter":"Terrace",
           "Plz":"Plaza",
           "PLAZA":"Plaza",
           "Prom":"Promenade",
           "Pkwy":"Parkway",
           "Pky":"Parkway",
           "Ln":"Lane",
           "LANE":"Lane",
           "lane":"Lane",
           "Ldg":"Landing",
           "Hl":"Hill",
           "Dr":"Drive",
           "drive":"Drive",
           "DRIVE":"Drive",
           "Cir":"Circle",
           "CIRCLE":"Circle",
           "Ct":"Court",
           "Blvd":"Boulevard",
           "boulevard":"Boulevard",
           "WAY":"Way",
           "WEST":"West",
           "Cres":"Crescent"
           }
def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected:
            street_types[street_type].add(street_name)


def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")


def audit(osmfile):
    osm_file = open(osmfile, encoding='utf8')
    street_types = defaultdict(set)
    for event, elem in ET.iterparse(osm_file):

        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])
    osm_file.close()
    return street_types

def audit_update(value):
    m = street_type_re.search(value)
    if m:
        street_type = m.group()
        if street_type not in expected:
            value = update_name(value,mapping)
    return value


                    #    for st_type, ways in st_types.iteritems():
                    #        for name in ways:
                    #            better_name = update_name(name, mapping)
                    #            print(name, "=>", better_name)


def update_name(name, mapping):
    objPattern = street_type_re.search(name)
    if objPattern:
        st_name = objPattern.group()
        if st_name in mapping.keys():
            newname = name[0:(len(name) - len(st_name))]
            name = newname + mapping[st_name]

    return name

def count_tags(filename):
    tags = {}
    for event, elem in ET.iterparse(filename):
        key = elem.tag
        if key in tags:
            tags[key] += 1
        else:
            tags[key] = 1
    return tags

def process_map_user(filename):
    users = set()
    for _, element in ET.iterparse(filename):
        user_id = element.get("uid")
        if (user_id != None)and(user_id not in users):
            users.add(user_id)

    return users

def process_map(file_in, pretty=False):
    # You do not need to change this file
    file_out = "{0}.json".format(file_in)
    data = []
    with codecs.open(file_out, "w") as fo:
        for _, element in ET.iterparse(file_in):
            el = shape_element(element)
            if el:
                data.append(el)
                if pretty:
                    fo.write(json.dumps(el, indent=2) + "\n")
                else:
                    fo.write(json.dumps(el) + "\n")
    return data


def shape_element(element):
    node = {}
    if element.tag == "node" or element.tag == "way":
        node["id"] = element.get("id")
        if element.tag == "node":
            node["type"] = "node"
        else:
            node["type"] = "way"
            node_ref = []
            for nd in element.iter("nd"):
                node_ref.append(nd.get("ref"))
            if node_ref:
                node["node_refs"] = node_ref
        node["visible"] = element.get("visible")
        created = {}
        for item in CREATED:
            created[item] = element.get(item)
        node["created"] = created
        lat = element.get("lat")
        lon = element.get("lon")
        if (lat and lon):
            node["pos"] = [float(lat), float(lon)]
        address = {}
        for tag in element.iter("tag"):
            k_value = tag.get("k")
            if bool(re.search(problemchars, k_value)):
                continue
            if bool(re.search(addrStart, k_value)):
                if not bool(re.search(doubleColon,k_value)):
                    key = k_value.split(":", 2)[1]
                    value = tag.get("v")
                    if (key == "street"):
                       value = audit_update(value)
                    address[key] = value
            else:
                node[k_value] = tag.get("v")
        if address:
            node["address"] = address

        return node
    else:
        return None



def test():
#    tags = count_tags(OSMFILE)
#    pprint.pprint(tags)
#    users = process_map_user(OSMFILE)
#    pprint.pprint(users)

#    st_types = audit(OSMFILE)
    #assert len(st_types) == 3
#    pprint.pprint(dict(st_types))

    data = process_map(OSMFILE, False)
#    pprint.pprint(data)





if __name__ == "__main__":
    test()