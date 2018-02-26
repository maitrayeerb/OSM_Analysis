#!/usr/bin/env python
"""
Use an aggregation query to answer the following question.

Extrapolating from an earlier exercise in this lesson, find the average
regional city population for all countries in the cities collection. What we
are asking here is that you first calculate the average city population for each
region in a country and then calculate the average of all the regional averages
for a country.
  As a hint, _id fields in group stages need not be single values. They can
also be compound keys (documents composed of multiple fields). You will use the
same aggregation operator in more than one stage in writing this aggregation
query. I encourage you to write it one stage at a time and test after writing
each stage.

Please modify only the 'make_pipeline' function so that it creates and returns
an aggregation  pipeline that can be passed to the MongoDB aggregate function.
As in our examples in this lesson, the aggregation pipeline should be a list of
one or more dictionary objects. Please review the lesson examples if you are
unsure of the syntax.

Your code will be run against a MongoDB instance that we have provided. If you
want to run this code locally on your machine, you have to install MongoDB,
download and insert the dataset. For instructions related to MongoDB setup and
datasets please see Course Materials.

Please note that the dataset you are using here is a different version of the
cities collection provided in the course materials. If you attempt some of the
same queries that we look at in the problem set, your results may be different.
"""
import pprint
from pymongo import MongoClient

def get_db(db_name):

    client = MongoClient('localhost:27017')
    db = client[db_name]
    return db


def make_pipeline():
    # complete the aggregation pipeline
    pipeline = [{"$match": {"country": {"$exists": 1}}},
                {"$unwind": "$isPartOf"},

                {"$group": {'_id': {"region": '$isPartOf', 'country': "$country"},
                            "reg_avg": {"$avg": "$population"},
                            'country_name': {"$addToSet": "$country"}}},

                {"$unwind": "$country_name"},

                {"$group": {'_id': "$country_name",
                            "avgRegionalPopulation": {"$avg": "$reg_avg"}}}

                ]
    return pipeline


def aggregate(db, pipeline):
    return [doc for doc in db.newyork.aggregate(pipeline)]

def getPostalCodes(db):
    pipeline = [{"$match":{"address.postcode":{"$exists":1}}},
                {"$group":{"_id":"$address.postcode", "city":{"$first":"$address.city"}, "count":{"$sum":1}}},
                {"$sort":{"count":-1}},
                {"$limit":20}]
    return [doc for doc in db.newyork.aggregate(pipeline)]

def getManhattanPostalCodes(db):
    pipeline = [{"$match":{"address.postcode":{"$exists":1},"address.postcode":{"$gt":10001},"address.postcode":{"$lt":10199}}},
                {"$group":{"_id":"$address.postcode", "city":{"$first":"$address.city"}, "count":{"$sum":1}}},
                {"$sort":{"count":-1}},
                {"$limit":10}]
    return [doc for doc in db.newyork.aggregate(pipeline)]

def getOverview(db):
    results = {}
    results["documents"] = db.newyork.find().count()
    results["nodes"] = db.newyork.find({"type":"node"}).count()
    results["ways"] = db.newyork.find({"type":"way"}).count()
    unique_users = len(db.newyork.distinct("created.user"))
    results["unique_users"] =unique_users
    return results


def getTopContributer(db):
    pipeline = [{"$group": {"_id": "$created.user", "count": {"$sum": 1}}},
                {"$sort": {"count":-1}},
                {"$limit": 10}]
    return [doc for doc in db.newyork.aggregate(pipeline)]

def getSinglePostUser(db):
    pipeline = [{"$group":{"_id":"$created.user", "count":{"$sum":1}}},
                {"$group":{"_id":"$count", "num_users":{"$sum":1}}},
                {"$sort":{"_id":1}}, {"$limit":1}]
    return [doc for doc in db.newyork.aggregate(pipeline)]

def getTopAmenities(db):
    pipeline = [{"$match":{"amenity":{"$exists":1}}},
                {"$group":{"_id":"$amenity","count":{"$sum":1}}},
                {"$sort":{"count":-1}},
                {"$limit":20}]
    return [doc for doc in db.newyork.aggregate(pipeline)]

def getPopularCuisine(db):
    pipeline = [{"$match":{"amenity":{"$exists":1}, "amenity":{"$eq":"restaurant"}}},
                {"$group":{"_id":"$cuisine", "count":{"$sum":1}}},
                {"$sort":{"count":-1}},
                {"$limit":10}]
    return [doc for doc in db.newyork.aggregate(pipeline)]

def getTopReligion(db):
    pipeline = [{"$match":{"amenity":{"$exists":1}, "amenity":{"$eq":"place_of_worship"}}},
                {"$group":{"_id":"$religion", "count":{"$sum":1}}},
                {"$sort":{"count":-1}},
                {"$limit":10}]
    return [doc for doc in db.newyork.aggregate(pipeline)]





if __name__ == '__main__':
    # The following statements will be used to test your code by the grader.
    # Any modifications to the code past this point will not be reflected by
    # the Test Run.
    db = get_db('osm')
    #pipeline = make_pipeline()
    #result = aggregate(db, pipeline)
    # pprint.pprint(result)
    result = getOverview(db)
    pprint.pprint(result)

    print("postal Codes")
    postalCodes = getPostalCodes(db)
    pprint.pprint(postalCodes)

    print("manhattan postal Codes")
    manhattanPostalCodes = getManhattanPostalCodes(db)
    pprint.pprint(manhattanPostalCodes)


    topUsers = getTopContributer(db)
    singlePostUser = getSinglePostUser(db)
    topAmenities = getTopAmenities(db)
    topCuisines = getPopularCuisine(db)
    topReligion = getTopReligion(db)

    print("top 10 users")
    pprint.pprint(topUsers)

    print("Number of single post users")
    pprint.pprint(singlePostUser)

    print("top 20 amenities")
    pprint.pprint(topAmenities)

    print("top 10 cuisines")
    pprint.pprint(topCuisines)

    print("top 10 religions")
    pprint.pprint(topReligion)







