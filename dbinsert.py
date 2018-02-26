"""
Complete the insert_data function to insert the data into MongoDB.
"""

import json
import re

def insert_data(data, db):
    db.newyork.insert(data)


if __name__ == "__main__":
    from pymongo import MongoClient

    client = MongoClient("mongodb://localhost:27017")
    db = client.osm

    with open('new-york_new-york.json') as f:
        text_data = f.read()
        text_data = '[' + re.sub(r'\}\s\{', '},{', text_data) + ']'
        data = json.loads(text_data)
        #data = json.loads(f.read())
        insert_data(data, db)
        print(db.newyork.find_one())




    print ("Data inserted")