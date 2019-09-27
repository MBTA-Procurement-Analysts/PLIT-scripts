# poDataInsertion.py
# Created by Nate O, modified by Mickey G for Project flextape use
# To Import PO data to mongodb, within the flextape pipeline

# On 11/14/2018, the script was modified to accomodate the addition
#   of 'PO Approval Date' field.

# On 09/26/2019, the script was modified to accomodate the addition
#   of 'Last_Updated' field.

# On 09/27/2019, the script was modified to acoomodate the addition
#   of 'Vendor_ID' field.

import pandas as pd
import pymongo
from pymongo import MongoClient
import sys
import time
import os

print("poDataInsertion.py taking over...")

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

filepath = filepathprefix + "po6/" + date + "/NO_RUBIX_PO_V2_6-" + date + ".xlsx" 

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
    for row in insertionItems.itertuples():
        if not uniquePOIDs[row.PO_No]:
            if pd.isna(row.PO_AprvDate):
                aprvDate = ""
            else:
                aprvDate = row.PO_AprvDate
            db.PO_DATA.update({'PO_No': row.PO_No}, {
                '$set': {
                    'PO_No': row.PO_No,
                    'Business_Unit': row.Business_Unit,
                    'Buyer': row.Buyer,
                    "Status": row.Status,
                    'PO_Date': row.PO_Date,
                    "Origin": row.Origin,
                    "Approved_By": row.Approved_By,
                    "Vendor": row.Vendor_Name,
                    "PO_Approval_Date": aprvDate,
                    "Last_Updated": row.Last_Dttm,
                    "Vendor_ID": row.Vendor,
                    "lines": []
                }
            }, upsert=True)
            uniquePOIDs[row.PO_No] = True
    
    for row in insertionItems.itertuples():
        db.PO_DATA.update({"PO_No": row.PO_No}, {
            '$push': {
                "lines": {
                    "Line_No": row.PO_Line,
                    "Mfg_ID": row.Mfg_ID,
                    "Mfg_Item_ID": row.Mfg_Itm_ID,
                    "Line_Total": row.Sum_Amount,
                    "Level_1": row.Level_1,
                    "Level_2": row.Level_2,
                    "Descr": row.Descr,
                    "Line_Descr": row.More_Info,
                    "FairMarkIt_Link": row.QuoteLink,
                    "Requisition_Data": {
                        "Req_ID": row.Req_ID,
                        "Req_Line": row.REQ_Line
                    },
                    "Quantity": row.PO_Qty
                }
            }})
        # db.REQ_DATA.find({"lines": {$elemMatch: {"Item": "02545903"}}})
    
    db.LAST_UPDATED.update({'dbname': "PO_DATA"}, {'$set': {'last_updated_time': time.time()}})
    
    print("--- PO Update Done ---")
