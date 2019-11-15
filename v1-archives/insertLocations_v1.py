# insertLocations.py
# Created by Nate O, modified by Mickey G for Project flextape use
# To Import Item Location metadata to mongodb, within the flextape pipeline

import pandas as pd
import pymongo
from pymongo import MongoClient
from tqdm import tqdm
import sys
import time
import os

if not sys.argv[1] and not sys.argv[2]:
    raise ValueError('Arguments Needed: Date String (mmddyyyy-hhmmss), Write Location: One of (dev, prod, both)')
date = sys.argv[1]
writelocation = ['dev', 'prod'] if sys.argv[2] == 'both' else [sys.argv[2]]

serverlocation = os.environ['RUBIXLOCATION']

if serverlocation == 'local':
    filepathprefix = "/home/rubix/Desktop/Project-Ducttape/data/"
elif serverlocation == 'ohio':
    filepathprefix = "/home/ubuntu/Projects/flextape/"
else:
    raise EnvironmentError('Environment Variable "RUBIXLOCATION" seems not to be set.')

filepath = filepathprefix + "itemloc/" + date + "/ITEM-LOCATION-" + date + ".xlsx"

print("--- Reading " + filepath + " ---")

# Reads Excel File
insertionItems = pd.read_excel(filepath, skiprows = 1)

# Strips Item ID whitespaces
insertionItems['Item'] = insertionItems['Item'].str.strip()

# Fills spaces in Column Names with underscores
insertionItems.columns = [c.replace(' ', '_') for c in insertionItems.columns]


# Initiates Mongodb connection
client = MongoClient()

for location in writelocation:
    # Find unique Item IDs, since Items with multiple location listings spans
    #   across multiple lines.
    # Dictionary is not necessary since we do not need to update any data for just
    #   once, and thus we don't need to check if we are writing multiple times for
    #   the same Item ID.
    uniqueItemArr = insertionItems['Item'].unique().tolist()
    dbname = 'rubix-' + serverlocation + '-' + location 
    print('Using database ' + dbname)
    db = client[dbname]

    for itemno in tqdm(uniqueItemArr):
        db.ITEM_DATA.update({'Item_No': itemno}, {
            '$set': {
                'Locations': [] 
                }
            }, upsert = True)
    
    
    for row in tqdm(insertionItems.itertuples()):
        db.ITEM_DATA.update({'Item_No': row.Item}, {
            '$push': {
                'Locations': {
                    'Unit': row.Unit,
                    'Area': row.Area,
                    'Level_1': row.Lev_1,
                    'Level_2': row.Lev_2,
                    'Level_3': row.Lev_3,
                    'Level_4': row.Lev_4,
                    'Qty': row.Qty
                }
            }
        })
    
    db.LAST_UPDATED.update({'dbname': "ITEM_DATA"}, {'$set': {'last_updated_time': time.time()}})
    #db.REQ_DATA.find({"lines": {$elemMatch: {"Item": "02545903"}}})
