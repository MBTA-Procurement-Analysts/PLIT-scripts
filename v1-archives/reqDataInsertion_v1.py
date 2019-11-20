# reqDataInsertion.py
# Created by Nate O, modified by Mickey G for Project flextape use
# To Import Req data to mongodb, within the flextape pipeline

import pandas as pd
import pymongo
from pymongo import MongoClient
from tqdm import tqdm
import sys
import time
import os

print("reqDataInsertion.py taking over...")

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

filepath = filepathprefix + "req-6/" + date + "/PLIT_REQ_6-" + date + ".xlsx"

print("--- Reading " + filepath + " ---")

insertionItems = pd.read_excel(filepath, skiprows = 1)
insertionItems['Due'] = insertionItems['Due'].fillna(pd.Timestamp('2001-01-01 00:00:00'))

client = MongoClient()
insertionItems.columns = [c.replace(' ', '_') for c in insertionItems.columns]
insertionItems.columns = [c.replace('/', '_') for c in insertionItems.columns]

print("--- Pushing Data to mongo ---")

print(list(insertionItems))


print("Removing Entries to be Updated...")

print("Adding Data...")

na_values = {"Address_2": ""}
insertionItems = insertionItems.fillna(value = na_values)

for location in writelocation:
    uniqueReqIDs = dict((reqno, False) for reqno in insertionItems['Req_ID'].unique().tolist())
    uniqueItemArr = insertionItems['Item'].unique().tolist()
    dbname = 'rubix-' + serverlocation + '-' + location
    print('Using database ' + dbname)
    db = client[dbname]
    for row in tqdm(insertionItems.itertuples()):
        if not uniqueReqIDs[row.Req_ID]:
            db.REQ_DATA.update({'REQ_No': row.Req_ID}, {
                '$set': {
                    'REQ_No': row.Req_ID,
                    'Account': row.Account,
                    'Business_Unit': row.Unit,
                    'Buyer': row.Buyer,
                    'Currency' : row.Currency,
                    'Department' : {
                        "Number": row.Dept_Loc
                    },
                    "Fund": row.Fund,
                    "Origin": row.Origin,
                    "REQ_Date": row.Req_Date,
                    "Requester": row.Requester,
                    "Ship_To": {
                        "Description": row.Descr,
                        "Address_1": row.Address_1,
                        "Address_2": row.Address_2,
                        "City": row.City,
                        "State": row.St,
                        "Zip_Code": row.Postal,
                        "Country": row.Cntry
                    },
                    "Status": row.Status,
                    "Approved_By" : row.By,
                    "Approved_On": row.Date,
                    "Vendor": {
                        "Number": row.Vendor,
                        "Name": row.Name
                    },
                    "lines": []
            }
            }, upsert = True)
            uniqueReqIDs[row.Req_ID] = True

    for row in tqdm(insertionItems.itertuples()):
        db.REQ_DATA.update({"REQ_No": row.Req_ID}, {
            '$addToSet': {
                "lines": {
                        "Line_No": row.Line,
                        "Unit_Price": row.Base_Price,
                        "Line_Total": row.Amount,
                        "Schedule_No": row.Sched_Num,
                        "Quantity": row.Req_Qty,
                        "Due_Date": row.Due,
                        "More_Info": row.More_Info,
                        "UOM": row.UOM,
                        "Item": row.Item
                    }
            }})
        # db.REQ_DATA.find({"lines": {$elemMatch: {"Item": "02545903"}}})

    db.LAST_UPDATED.update({'dbname': "REQ_DATA"}, {'$set': {'last_updated_time': time.time()}})


    print("Req Update Done!")

