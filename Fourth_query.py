#!/usr/bin/env python
# coding: utf-8

import pymongo
import csv

my_client = pymongo.MongoClient("mongodb://localhost:27017/")
my_db = my_client["dvdrental"]

def get_film_id_list(c_id):
    global my_db
    film_ids = []
    cust_rents = list(my_db.rental.find({'customer_id':c_id}, {'_id':0, 'customer_id':1, 'inventory_id':1}))
    for rent in cust_rents:
        inv_id = rent.get('inventory_id')
        ins = list(my_db.inventory.find({'inventory_id':inv_id}, {'_id':0, 'inventory_id':1, 'film_id':1}))
        fid = ins[0].get('film_id')
        if not(fid in film_ids):
            film_ids.append(fid)
    return film_ids

def get_commons(films_c1, films_c2):
    difs = []
    n = len(films_c1)
    if (n != 0):
        k = 0.0
        for film in films_c2:
            if (film in films_c1):
                k += 1
            else:
                difs.append(film)
        return [difs, k / n]

    else: 
        return [[], 0]

def get_film_info(f_id, p):
    infos = list(my_db.film.find({'film_id':f_id}, {'_id':0, 'film_id':1, 'title':1, 'release_year':1}))[0]
    cat_id = list(my_db.film_category.find({'film_id':f_id}, {'_id':0, 'film_id':1, 'category_id':1}))[0]            .get('category_id')
    cat_name = list(my_db.category.find({'category_id':cat_id}, {'_id':0, 'category_id':1, 'name':1}))[0]            .get('name')
    info = [0, '', '', 0, 0.0]
    info[0] = f_id
    info[1] = infos.get('title')
    info[2] = cat_name
    info[3] = infos.get('release_year')
    info[4] = p
    return info

films_for_id = my_db.film.find({}, {'_id':0, 'film_id':1})
max_film_id = films_for_id.sort('film_id', -1).limit(1)[0].get('film_id')

c_id = int(input("Enter ID of desired customer: "))
films = get_film_id_list(c_id)

scores = [0] * max_film_id
customers = list(my_db.customer.find({}, {'_id':0, 'customer_id':1}))
for customer in customers:
    other_c_id = customer.get('customer_id')
    if (c_id != other_c_id):
        other_films = get_film_id_list(other_c_id)
        com = get_commons(films, other_films)
        com_p = com[1]
        difs = com[0]
        for dif in difs:
            scores[dif - 1] += com_p

recs = [[i + 1, scores[i]] for i in range(max_film_id)]

for i in range(max_film_id - 1):
    for j in range(i + 1, max_film_id):
        if recs[i][1] < recs[j][1]:
            recs[i][1], recs[j][1] = recs[j][1], recs[i][1]
            recs[i][0], recs[j][0] = recs[j][0], recs[i][0] 

data = [[0, '', '', 0, 0.0] for i in range(max_film_id + 1)]
data[0] = ['ID', 'Title', 'Category', 'Year', 'Metric']
for i in range(max_film_id):
    data[i + 1] = get_film_info(recs[i][0], round(recs[i][1], 3))

with open('fourth_query_report.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerows(data)

my_client.close()

