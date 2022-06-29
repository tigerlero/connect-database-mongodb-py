import os
import json
import pymongo
import pandas as pd
import numpy as np
from tqdm import tqdm
from pprint import pprint

'''
    Για την χρήση των παραπάνω βιβλιοθηκών, να έχετε εγκατεστημένη την Python3 
    μέσω του περιβάλλοντος Anaconda3 (https://www.anaconda.com/). Αναλυτικότερα:
    * Pandas (included in anaconda): conda install pandas
    * NumPy (included in anaconda):  conda install numpy
    * PyMongo:  conda install pymongo
    * tqdm:     conda install tqdm

    Αφού ορίσετε όνομα για την βάση στην οποία θα φορτωθούν τα δεδομένα (Γραμμή 21)
    και τα host, port του MongoDB server (στην περίπτωση που δεν είναι τα installation 
    defaults) εκτελέστε το σκριπτάκι **στον ίδιο φάκελο** με τα CSVs γράφοντας στο terminal:

                                python create_imdb.py
'''


MONGO_DB_NAME = "βάλτε εδώ το όνομα της βάσης στην οποία θα φορτωθούν τα δεδομένα (π.χ. imdb)"


def mongoimport(mongoclient, csv_path, db_name, col_name):
    """ 
        Imports a CSV file to a mongo collection ```db_name``` of a database ```db_name```
        Returns: count of the documents in the new collection
    """    
    db = mongoclient.get_database(name=db_name)
    db_col = db.get_collection(name=col_name)
    
    db_col.delete_many({})   # DROP COLLECTION IF EXISTS
    
    inserted_documents = 0
    
    for chunk in tqdm(pd.read_csv(csv_path, chunksize=10**6), desc=f"Inserting at {db_name}.{col_name}"):
        payload = json.loads(chunk.to_json(orient='records'))   
        insertion_result = db_col.insert_many(payload) # INSERT INTO COLLECTION...
        inserted_documents += len(insertion_result.inserted_ids)
        
    return inserted_documents




if __name__ == '__main__':
    # Connect to MongoDB database
    mongoclient = pymongo.MongoClient(host='127.0.0.1', port=27017)

    # 1. Import 'title_ratings'
    insertion_result = mongoimport(mongoclient, os.path.join('.', 'title_ratings.csv'), MONGO_DB_NAME, 'title_ratings')
    print (f'Inserted {insertion_result} Documents.')
    # 2. Import 'title_basics'
    insertion_result = mongoimport(mongoclient, os.path.join('.', 'title_basics.csv'), MONGO_DB_NAME, 'title_basics')
    print (f'Inserted {insertion_result} Documents.')


    # Preprocess 'genres' field of title_basics collection
    db = mongoclient.get_database(name=MONGO_DB_NAME)
    db_col = db.get_collection(name='title_basics')


    group = db_col.find({'genres':{'$ne': None}})
    for grp in tqdm(group, total=group.count(), position=0):
        myquery = { '_id': grp['_id'] }
        try:
            newvalues = { '$set': {'genres': grp['genres'].split(',')}}
        except AttributeError:
            continue

        db_col.update_one(myquery, newvalues)
