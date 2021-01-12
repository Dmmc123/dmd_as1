#!/usr/bin/env python
# coding: utf-8

import psycopg2
import pymongo
import json

# dealing with datatime formats so mongodb can accept dumped json
class datetime_encoder(json.JSONEncoder):
    def default(self, obj):
        try:
            return super(datetime_encoder, obj).default(obj)
        except(TypeError):
            return str(obj)

login_data = []
with open("Login_data.txt", "r") as file:
    for line in file:
        login_data.append(line[:-1])

con = psycopg2.connect(database="dvdrental", user=login_data[0],
                       password=login_data[1], host="127.0.0.1", port="5432")
cur = con.cursor()

# getting list of all tables
tables = []
cur.execute("""SELECT table_name FROM information_schema.tables    WHERE table_schema = 'public'""")
for table in cur.fetchall():
    tables.append(table[0])

# getting the list of columns for every table
columns = []
for table in tables:
    cur.execute("""Select * FROM {}""".format(table))
    columns.append([desc[0] for desc in cur.description])

my_client = pymongo.MongoClient("mongodb://localhost:27017/")
my_db = my_client["dvdrental"]

for i in range(len(tables)):
    # retrieving data from psql 
    cur.execute("""SELECT * FROM {}""".format(tables[i]))
    results = []
    for row in cur.fetchall():
        results.append(dict(zip(tuple(columns[i]), row)))
     
    # representing retrieved data in json format so mongodb can eat it
    data_str = json.dumps(results, cls = datetime_encoder, indent = 2)
    data_tb = json.loads(data_str)
    
    # mongodb eats the data
    temp_tb = my_db[tables[i]]
    temp_tb.delete_many({})
    temp_tb.insert_many(data_tb)

cur.close()
my_client.close()

