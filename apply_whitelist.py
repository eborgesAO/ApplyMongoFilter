from pymongo import MongoClient
from connect_db import connect_to_mongodb_src
import os
import pandas

#paths
whitelist = "csv/whitelist_cat.csv"
blacklist = "csv/blacklist_cat.csv"

# connection
dbname = "Skittles-Blacklisting-Prog"
db=connect_to_mongodb_src(dbname)

#collections
whitelist_collection_name="whitelist"
src_name="src_listing"
filtered=db["src_filtered"]


def main():
    if input("Import csv?   y/n ")=="y":
        collection=db[whitelist_collection_name]
        collection.drop()
        command=f"mongoimport --host localhost --port 27017 --db {dbname} --collection {whitelist_collection_name} --type csv --file {whitelist} --headerline"
        os.system(command)

# menu=input("Use: \n1=Whitelist\n2=Blacklist")

    criterias=[]
    cv = pandas.read_csv(whitelist)
    criterias=cv['path'].tolist()
    breakpoint()

    for criteria in criterias:
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
    
    createFiltered(collection,query,filtered)  



def createFiltered(collection,query,new_coll):    
    result = collection.find(query)
    batch=[]
    i=0
    new_coll.drop()
    for each in result:
        batch.append(each)
        i=i+1
        if i % 250000 ==0:
            print(i)
            new_coll.insert_many(batch)
            batch=[]  
    print(i)
    new_coll.insert_many(batch)


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