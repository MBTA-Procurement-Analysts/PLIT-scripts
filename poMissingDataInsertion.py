# poMissingDataInsertion.py
# Created by Christopher M. for Project flextape use
# To Import PO data to mongodb, within the flextape pipeline
  
  # On 4/2/2019, the script was created in order to automate emails for missing PO's
   
  
import pandas as pd
import pymongo
from pymongo import MongoClient
from tqdm import tqdm
import sys
import time
import os
 
print("poMissingDataInsertion.py taking over...")
 
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
filepath = filepathprefix + "po-status-vendors/" + date + "/CM_PO_OPN_BY_BU_BY_DATE_FS-" + date + ".xlsx"
 
print("--- Reading " + filepath + " ---")
 
insertionItems = pd.read_excel(filepath, skiprows=1)
 
client = MongoClient()
insertionItems.columns = [c.replace(' ', '_') for c in insertionItems.columns]
insertionItems.columns = [c.replace('/', '_') for c in insertionItems.columns]
insertionItems.columns = [c.replace('.', '') for c in insertionItems.columns]
 
print("--- Pushing Data to mongo ---")
 
print(insertionItems.columns)

for location in writelocation:
    dbname = 'rubix-' + serverlocation + '-' + location
    print('Using database ' + dbname)
    db = client[dbname]
    uniquePOIDs = dict((pono, False) for pono in insertionItems['PO_No'].unique().tolist())  
    for row in tqdm(insertionItems.itertuples()):
        if not uniquePOIDs[row.PO_No]:
            db.MISSING_PO.update({'PO_No': row.PO_No}, {
                '$set': {
                
                    "Vendor": row.Vendor,
                  
                }
            }, upsert=True)
            uniquePOIDs[row.PO_No] = True
 
 
    db.LAST_UPDATED.update({'dbname': "PO_DATA"}, {'$set': {'last_updated_time': time.time()}})
 
    print("--- PO Update Done ---")
