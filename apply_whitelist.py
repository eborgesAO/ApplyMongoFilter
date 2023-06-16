from pymongo import MongoClient
from connect_db import connect_to_mongodb_src
import pandas as pd
import numpy as np
import os

#paths
whitelist = "csv/whitelist_cat.csv"
blacklist = "csv/blacklist_cat.csv"

# connection
dbname = "Skittles-Blacklisting-Prog"
db=connect_to_mongodb_src(dbname)

#collections
whitelist_collection_name = "whitelist"
blacklist_collection_name = "blacklist"
src_name = "src_listing"
whitelist_name = "src_whitelist_filtered"
filtered = db["src_whitelist_filtered"]
filtered_filtered = db["src_final_filter"]


def main():
    if input("Import multiple csv?   y/n ")=="y":
        collection=db[whitelist_collection_name]
        collection.drop()
        command=f"mongoimport --host localhost --port 27017 --db {dbname} --collection {whitelist_collection_name} --type csv --file {whitelist} --headerline"
        os.system(command)

        collection=db[blacklist_collection_name]
        collection.drop()
        command=f"mongoimport --host localhost --port 27017 --db {dbname} --collection {blacklist_collection_name} --type csv --file {blacklist} --headerline"
        os.system(command)
    if input("Run whitelist?   y/n ")=="y":
        whitelisting()
    if input("Run blacklist?   y/n ")=="y":
        blacklisting()

    print("It Finished!")


def createFiltered(collection,query,new_coll,x):    
    batch=[]
    i=0
     
    if x == 0:
        result = collection.find(query)
        for each in result:
            batch.append(each)
            i=i+1
            if i % 250000 ==0:
                print(i)
                new_coll.insert_many(batch)
                batch=[]  
        print(i)
        if batch != []:
            new_coll.insert_many(batch)
        else:
            print(f"Last batch empty: {i}")
    else:
        new_coll.delete_many(query)


def whitelisting():

    filtered.drop()
    white_c = pd.read_csv(whitelist)
    for criteria in white_c['path']:
        if pd.isnull(criteria) or isinstance(criteria, np.float64):
            continue
        collection = db[src_name]
        if criteria [-1] =="/":
            criteria = criteria[:-1]
            criteria = f"{criteria}.*"
            query = {
                'filepath': {
                    '$regex': criteria
                }
            }
        else:
            query = {
                'filepath': criteria
            }
        print(query)
        createFiltered(collection,query,filtered,0)  
    for criteria in white_c['star']:
        if pd.isnull(criteria) or isinstance(criteria, np.float64):
            continue
        criteria = criteria.replace('.', '\\.')
        criteria = criteria.replace('*', '(?:(?!/)[^/])+')
        criteria = f"{criteria}$"
        collection = db[src_name]
        query = {
            'filepath': {"$regex": criteria}, "filetype":"f"
                        }
        print(query)
        createFiltered(collection,query,filtered,0)


def blacklisting():

    filtered_filtered.drop()
    copy = filtered.find()
    filtered_filtered.insert_many(copy)


    black_c = pd.read_csv(blacklist)
    for criteria in black_c['path']:
        if pd.isnull(criteria) or isinstance(criteria, np.float64):
            continue
        collection = db[whitelist_name]
        if criteria [-1] =="/":
            criteria = criteria[:-1]
            criteria = f"{criteria}.*"
            query = {
                'filepath': 
                             {
                    '$regex': criteria
                }
            }
        else:
            query = {
                'filepath': criteria
            }
        print(query)
        createFiltered(collection,query,filtered_filtered,1)  
    for criteria in black_c['star']:
        if pd.isnull(criteria) or isinstance(criteria, np.float64):
            continue
        criteria = criteria.replace('.', '\\.')
        criteria = criteria.replace('*', '(?:(?!/)[^/])+')
        criteria = f"{criteria}$"
        collection = db[whitelist_name]
        query = {
            'filepath': {"$regex": criteria}, "filetype":"f"
                        }
        print(query)
        createFiltered(collection,query,filtered_filtered,1)

main()
















#menu for both, use white & apply black to results of first
# if menu == 2 :
#     query = {
#         'filename': {
#             '$not': {
#                 '$regex': '/proj/radar.*'
#             }
#         }
#     }