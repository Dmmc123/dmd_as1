#!/usr/bin/env python
# coding: utf-8

import pymongo
import csv

my_client = pymongo.MongoClient("mongodb://localhost:27017/")
my_db = my_client["dvdrental"]

rent_data = my_db.rental.find({}, {'_id':0, 'rental_date':1})
max_year = rent_data.sort('rental_date', -1).limit(1)
max_year = max_year[0].get('rental_date')[:4]         

customers = list(my_db.customer.find({}, {'_id':0, 'customer_id':1, 'first_name':1, 'last_name':1}))
rents = list(my_db.rental.find({}, {'_id':0, 'rental_date':1, 'inventory_id':1, 'customer_id':1}))
inv = list(my_db.inventory.find({}, {'_id':0, 'inventory_id':1, 'film_id':1}))
film_cat = list(my_db.film_category.find({}, {'_id':0, 'film_id':1, 'category_id':1}))  

data = [['Customer ID', 'First name', 'Last name']]

for customer in customers:
    unique_cats = []
    for rent in rents:
        if (customer.get('customer_id') == rent.get('customer_id')            and rent.get('rental_date')[:4] == max_year):
            for ins in inv:
                if (rent.get('inventory_id') == ins.get('inventory_id')):
                    for cats in film_cat:
                        if (ins.get('film_id') == cats.get('film_id')):
                            cat = cats.get('category_id')
                            if not(cat in unique_cats):
                                unique_cats.append(cat)
    if (len(unique_cats) > 1): 
        data.append([customer.get('customer_id'),
                     customer.get('first_name'),
                     customer.get('last_name')])

with open('first_query_report.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerows(data)

my_client.close()
