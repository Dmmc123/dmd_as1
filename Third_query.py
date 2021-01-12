#!/usr/bin/env python
# coding: utf-8

import pymongo
import csv

my_client = pymongo.MongoClient("mongodb://localhost:27017/")
my_db = my_client["dvdrental"]

films = list(my_db.film.find({}, {'_id':0, 'film_id':1, 'title':1}))

n = len(films)
data = [[0]*4 for i in range(n + 1)]
data[0] = ['ID', 'Title', 'Category', 'Number of rents']

for i in range(n):
    fid = films[i].get('film_id')
    inv_id = list(my_db.inventory.find({'film_id':fid}, {'_id':0, 'inventory_id':1, 'film_id':1}))
    cat_id = list(my_db.film_category.find({'film_id':fid}, {'_id':0, 'film_id':1, 'category_id':1}))[0]                .get('category_id')
    cat_name = list(my_db.category.find({'category_id':cat_id}, {'_id':0, 'category_id':1, 'name':1}))[0]                .get('name')
    data[i + 1][0] = fid
    data[i + 1][1] = films[i].get('title')
    data[i + 1][2] = cat_name
    data[i + 1][3] = len(inv_id)

with open('third_query_report.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerows(data)

