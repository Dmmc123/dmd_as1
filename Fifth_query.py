#!/usr/bin/env python
# coding: utf-8

import pymongo
import csv

def get_actor_info(a_id, degree):
    actor = list(my_db.actor.find({'actor_id':a_id}, {'_id':0, 'actor_id':1, 'first_name':1, 'last_name':1}))[0]
    fname = actor.get('first_name')
    lname = actor.get('last_name')
    return [a_id, fname, lname, degree]

import time
start_time = time.time()

my_client = pymongo.MongoClient("mongodb://localhost:27017/")
my_db = my_client["dvdrental"]

actors = list(my_db.actor.find({}, {'_id':0, 'actor_id':1, 'first_name':1, 'last_name':1}))
film_ids = list(my_db.film.find({}, {'_id':0, 'film_id':1}))

n = len(actors)
m = 10**6
actor_costs = [[m]*n for i in range(n)]
for i in range(n):
    actor_costs[i][i] = 0

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
                    actor_costs[aid1 - 1][aid2 - 1] = 1
                    actor_costs[aid2 - 1][aid1 - 1] = 1

for k in range(n):
    for i in range(n):
        for j in range(n):
            if actor_costs[i][k] + actor_costs[k][j] < actor_costs[i][j]:
                actor_costs[i][j] = actor_costs[i][k] + actor_costs[k][j]

for i in range(n):
    for j in range(n):
        if (actor_costs[i][j] == m):
            actor_costs[i][j] = 0

# generates matrix of degrees of separation for all actors
# starting from actor_id = 1
with open('fifth_query_report_all.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerows(actor_costs)

a_id = int(input("Enter ID of desired actor: "))
actors_slice = actor_costs[a_id - 1]
data_actor = []
data_actor.append(['ID', 'First name', 'Last name', 'Degree of separation'])
for i in range(n):
    if (i + 1 != a_id):
        data_actor.append(get_actor_info(i + 1, actors_slice[i]))

# generates output file for a particular actor
with open('fifth_query_report_actor.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerows(data_actor)

my_client.close()

