# poInReqLines.py
# Created by Mickey G for Project flextape use
# To Import PO # and Line number data to REQ , within the flextape pipeline

import pandas as pd
import pymongo
from pymongo import MongoClient
from tqdm import tqdm
import sys
import time
import os

if not sys.argv[1] and not sys.argv[2]:
    raise ValueError(
        'Arguments Needed: Date String (mmddyyyy-hhmmss), Write Location: One of (dev, prod, both)')
date = sys.argv[1]
writelocation = ['dev', 'prod'] if sys.argv[2] == 'both' else [sys.argv[2]]

serverlocation = os.environ['RUBIXLOCATION']

if serverlocation == 'local':
    filepathprefix = "/home/rubix/Desktop/Project-Ducttape/data/"
elif serverlocation == 'ohio':
    filepathprefix = "/home/ubuntu/Projects/flextape/"
else:
    raise EnvironmentError(
        'Environment Variable "RUBIXLOCATION" seems not to be set.')

filepath = filepathprefix + "req_line_po_line/" + \
    date + "/PLIT_REQ_LINE_PO_LINE-" + date + ".xlsx"

print("--- Reading " + filepath + " ---")

insertionItems = pd.read_excel(filepath, skiprows=1)

client = MongoClient()
insertionItems.columns = [c.replace(' ', '_') for c in insertionItems.columns]
insertionItems.columns = [c.replace('/', '_') for c in insertionItems.columns]
insertionItems.columns = [c.replace('.', '') for c in insertionItems.columns]
insertionItems = insertionItems.rename(
    columns={"Line": "REQ_Line", "Line1": "PO_Line"})

print("--- Pushing Data to mongo ---")

print("Adding Data...")
for location in writelocation:
    dbname = 'rubix-' + serverlocation + '-' + location
    print('Using database ' + dbname)
    db = client[dbname]
    for row in tqdm(insertionItems.itertuples()):
        db.REQ_DATA.update_one({"$and": [{"REQ_No": row.Req_ID},
                                            {"Business_Unit": row.Unit}]},
                                 {"$set": {"lines.$[l].PO": {
                                     "PO_Number": row.PO_No,
                                     "Line_No": row.PO_Line}}},
                                 array_filters=[{"l.Line_No": row.REQ_Line}])

    db.LAST_UPDATED.update({'dbname': "PO_DATA_IN_REQ"}, {
                           '$set': {'last_updated_time': time.time()}})

    print("PO in Req Lines Update Done!")
