import os
from pymongo import MongoClient
from dotenv import load_dotenv

if not load_dotenv(".env.txt"):
    raise FileNotFoundError

USER = os.getenv("MONGO_USER")
PASSWORD = os.getenv("MONGO_PASSWORD")

def get_database(db_name):
    """Function that return db connection"""
    
    CONNECTION_STRING =\
        f"mongodb+srv://{USER}:{PASSWORD}@cluster0.c5png4c.mongodb.net/?retryWrites=true&w=majority"
        
    client = MongoClient(CONNECTION_STRING)
    
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)
    
    return client[db_name]

if __name__ == "__main__":
    dbname = get_database("cv")
