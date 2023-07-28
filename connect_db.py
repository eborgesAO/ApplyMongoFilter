from pymongo import MongoClient

def connect_to_mongodb_src(database_name):
    # Connect to the MongoDB server running on localhost
    client = MongoClient('mongodb://localhost:27017/')

    # Access the database
    db = client[database_name]


    return db