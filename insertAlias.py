# insertAlias.py
# Created by Nate O, modified by Mickey G for Project flextape use
# To Import Item metadata to mongodb, within the flextape pipeline

import pandas as pd
import pymongo
from pymongo import MongoClient
from tqdm import tqdm
import sys
import time
import os

print("insertAlias.py taking over...")

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

filepath = filepathprefix + "item_alias/" + date + "/ITEM-ALIAS-" + date + ".xlsx"

print("--- Reading " + filepath + " ---")

# Reads Excel File
insertionItems = pd.read_excel(filepath, skiprows = 1)

# Strips Item ID whitespaces
insertionItems['Item'] = insertionItems['Item'].str.strip()

# Fills spaces in Column names with underscores
insertionItems.columns = [c.replace(' ', '_') for c in insertionItems.columns]


# Initiates Mongodb connection
client = MongoClient()

for location in writelocation:
    # Find unique Item IDs, since Items with multiple viable substitues spans across
    #   multiple lines.
    uniqueItemIDs = dict((itemno, False) for itemno in insertionItems['Item'].unique().tolist())
    dbname = 'rubix-' + serverlocation + '-' + location 
    print('Using database ' + dbname)
    db = client[dbname]

    for itemno, _ in tqdm(uniqueItemIDs.items()):
        db.ITEM_DATA.update({'Item_No': itemno}, {
            '$set': {
                'Viable_Subs': []
                }
            }, upsert = True)
    
    for row in tqdm(insertionItems.itertuples()):
        if not uniqueItemIDs[row.Item]:
            db.ITEM_DATA.update({'Item_No': row.Item}, {
                    '$set': {
                        'Item_Description': row.Descr,
                        'Item_Group': {
                            "Group_Number": row.Item_Group,
                            "Group_Description": row.GDescr
                            },
                        'Status': row.Status_Current,
                        'UOM': row.Std_UOM,
                        'Status_Date': row.Itm_Status_Dt,
                        }
                    }, upsert = True)
            uniqueItemIDs[row.Item] = True
    
        db.ITEM_DATA.update({'Item_No': row.Item}, {
            '$push': {
                'Viable_Subs': {
                    'Mfg_ID': row.Mfg_ID,
                    'Mfg_Item_ID': row.Mfg_Itm_ID
                    }
                }
            })
    db.LAST_UPDATED.update({'dbname': "ITEM_DATA"}, {'$set': {'last_updated_time': time.time()}})
    #db.REQ_DATA.find({"lines": {$elemMatch: {"Item": "02545903"}}})
