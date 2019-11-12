# dashboardInsertion.py
# Created by Nate O, modified by Mickey G for Project flextape use
# To Import Req data to mongodb, within the flextape pipeline

import pandas as pd
import pymongo
from pymongo import MongoClient
from tqdm import tqdm
import sys
import time
import os

print("dashboardInsertion.py taking over...")

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

filepath = filepathprefix + "worklist/" + date + "/NO_WORKLIST-" + date + ".xlsx"

print("--- Reading " + filepath + " ---")

insertionItems = pd.read_excel(filepath, skiprows = 1)

client = MongoClient()
insertionItems.columns = [c.replace(' ', '_') for c in insertionItems.columns]
insertionItems.columns = [c.replace('-', '_') for c in insertionItems.columns]
insertionItems.columns = [c.replace('?', '') for c in insertionItems.columns]

for location in writelocation:
    dbname = 'rubix-' + serverlocation + '-' + location
    print('Using database ' + dbname)
    db = client[dbname]
    # We drop the collection everytime we refresh the dashboard data, since
    #   the upstream query is a snapshot and records come and go in the query.
    # There will be no way to detect if the record is not in the query anymore
    #   than removing anything that is not in the current query; and dropping
    #   the entire collection and then adds the current items is faster.
    db.DASHBOARD_DATA.drop()
    for row in tqdm(insertionItems.itertuples()):
        if pd.isna(row.Transmitted_Time):
        	tsmtTime = ""
        else:
        	tsmtTime = row.Transmitted_Time
        db.DASHBOARD_DATA.update({"REQ_No": row.Requisition_ID},
                                 {"$set": {
                                     'Req_ID': row.Requisition_ID,
                                     'Business_Unit': row.Business_Unit,
                                     'Buyer': row.Buyer,
                                     'Hold_From_Further_Processing': row.Hold_From_Further_Processing,
                                     'Hold_Status': row.Hold_Status,
                                     'Sourcing': row.Sourcing,
                                     'Lines_Not_Sourced': row.Nbr_Lines_Not_Sourced,
                                     'Out_To_Bid': row.Out_To_Bid,
                                     'Transmitted': row.Transmitted,
                                     'Transmitted_Time': tsmtTime
                                 }}, upsert=True)

    db.LAST_UPDATED.update({'dbname': "DASHBOARD_DATA"}, {'$set': {'last_updated_time': time.time()}})
