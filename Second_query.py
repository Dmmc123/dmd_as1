#!/usr/bin/env python
# coding: utf-8

import pymongo
import csv

my_client = pymongo.MongoClient("mongodb://localhost:27017/")
my_db = my_client["dvdrental"]

actors = list(my_db.actor.find({}, {'_id':0, 'actor_id':1, 'first_name':1, 'last_name':1}))
film_ids = list(my_db.film.find({}, {'_id':0, 'film_id':1}))

n = len(actors)
rep = [[0]*n for i in range(n)]

for film in film_ids:
    fid = film.get('film_id')
    film_cast = list(my_db.film_actor.find({'film_id':fid}, {'_id':0, 'actor_id':1}))
    cn = len(film_cast)
    if (cn > 1):
        for i in range(cn - 1):
            aid1 = film_cast[i].get('actor_id')
            for j in range(i + 1, cn):
                aid2 = film_cast[j].get('actor_id')
                if (aid1 != aid2):
                    rep[aid1 - 1][aid2 - 1] += 1
                    rep[aid2 - 1][aid1 - 1] += 1

# writes n x n matrix where each row and column
# start from actor_id = 1 to actor_id = max(actor_id)
with open('second_query_report.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerows(rep)

my_client.close()

