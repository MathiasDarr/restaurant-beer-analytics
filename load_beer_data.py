import csv
import os
import csv
from utilities.cassandra_utilities import createCassandraConnection, createKeySpace
import numpy as np
from numpy.random import choice

import json

def create_beer_products_table():
    create_customers_table = """CREATE TABLE IF NOT EXISTS beers(
        brewery_id int,
        name text,
        brewery_state text,
        brewery_city text,
        style_name text,
        category text,
        PRIMARY KEY(brewery_id,name));
        """
    dbsession.execute(create_customers_table)


def populate_beers():
    JSON_FILE = 'data/beer.json'

    with open(JSON_FILE) as f:
        data = json.load(f)
    return data

dbsession = createCassandraConnection()
createKeySpace("ks1", dbsession)
try:
    dbsession.set_keyspace('ks1')
except Exception as e:
    print(e)
beers = populate_beers()

create_beer_products_table()

insert_beer = """insert into beers(brewery_id, name, brewery_state, brewery_city, style_name, category) values(%s,%s,%s,%s,%s,%s)"""
for beer in beers:


    fields = beer['fields']
    if 'brewery_id' not in fields:
        brewery_id = -1
    else:
        try:
            brewery_id = int(fields['brewery_id'])
        except:
            brewery_id = -1

    if 'name' not in fields:
        name = 'null'
    else:
        name = fields['name']

    if 'cat_name' not in fields:
        category = 'null'
    else:
        category = fields['cat_name']

    if 'state' not in fields:
        state = 'null'
    else:
        state = fields['state']

    if 'style_name' not in fields:
        style_name = 'null'
    else:
        style_name = fields['style_name']

    if 'city' not in fields:
        city = 'null'
    else:
        city = fields['city']


    dbsession.execute(insert_beer,[brewery_id, name, state, city, style_name, category])



