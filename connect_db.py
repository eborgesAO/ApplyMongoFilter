from pymongo import MongoClient

def connect_to_mongodb_src(dbname):
    # Connect to the MongoDB server running on localhost
    client = MongoClient('mongodb://localhost:27017/')

    # Access the database
    db = client[dbname]


    return db

# Skittles-Blacklisting-Prog
# src_listing