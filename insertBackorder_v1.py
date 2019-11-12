# insertBackorder.py
# Created by Nate O, modified by Mickey G for Project flextape use
# To Import Item backorder metadata to mongodb, within the flextape pipeline

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

filepath = filepathprefix + "itembackorder/" + date + "/ITEM-BACKORDER-" + date + ".xlsx"

print("--- Reading " + filepath + " ---")

# Reads Excel File
# Type of ItemID has to be specified since Pandas will think columns 
#   without alphabets are ints
insertionItems = pd.read_excel(filepath, skiprows = 1, dtype = {'Item': str})

# Strips Item ID whitespaces
insertionItems['Item'] = insertionItems['Item'].str.strip()

# Fills spaces in Column names with underscores
insertionItems.columns = [c.replace(' ', '_') for c in insertionItems.columns]

# Initiates Mongodb connection
client = MongoClient()

print("Adding Data...")


for location in writelocation:
    dbname = 'rubix-' + serverlocation + '-' + location 
    print('Using database ' + dbname)
    db = client[dbname]
    # Resets all Backorder Amount that != 0 to 0, since the original query will
    #   not include 0-amount entries; thus if the amount goes to 0 there is no 
    #   entry in the query.
    # Multi Flag set as true to cover all Entries
    db.ITEM_DATA.update({'Total_Backorder': {'$ne': 0}}, {'$set' : {'Total_Backorder': 0}}, multi = True)

    for row in tqdm(insertionItems.itertuples()):
        db.ITEM_DATA.update({'Item_No': row.Item}, 
                {'$set': 
                    {'Total_Backorder': row.Qty_Req}}) 
    
    db.LAST_UPDATED.update({'dbname': "ITEM_DATA"}, {'$set': {'last_updated_time': time.time()}})
    #db.REQ_DATA.find({"lines": {$elemMatch: {"Item": "02545903"}}})
