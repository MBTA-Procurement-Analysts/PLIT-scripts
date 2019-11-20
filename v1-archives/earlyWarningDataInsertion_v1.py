# earlyWarningDataInsertion.py
# Created by Christopher M
# To Import PO data to mongodb, within the flextape pipeline

import pandas as pd
import pymongo
from pymongo import MongoClient
from tqdm import tqdm
import sys
import time
import os

print("earlyWarningDataInsertion.py taking over...")

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

filepath = filepathprefix + "early_warning/" + date + "/SLT_CPTL_EARLYWARNING-" + date + ".xlsx" 

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
    db.EARLY_WARNING.drop()   
    print("--- DATABASE CLEARED IN ANTICIPATION OF NEW DATA ---")
#    uniquePOIDs = dict((pono, False) for pono in insertionItems['PO_No'].unique().tolist())
    for row in tqdm(insertionItems.itertuples()):
#        if not uniquePOIDs[row.PO_No]:
            if pd.isna(row.Date):
                Date_Approved = ""
            else:
                Date_Approved=row.Date

            if pd.isna(row.PO_Date):
                PO_Date = ""
            else:
                PO_Date = row.PO_Date

            if pd.isna(row.Req_Approval_Date):
                Req_Approval_Date = ""
            else:
                Req_Approval_Date = row.Req_Approval_Date

            if pd.isna(row.Req_Created_Date):
                Req_Created_Date= ""
            else:
                Req_Created_Date = row.Req_Created_Date


#            db.EARLY_WARNING.update({'Req_Descr': row.Req_Descr}, {
#                '$set': {
#                    'Business_Unit': row.Unit,
#                    'Req_ID': row.Req_ID,
#                    'Req_Created_Date': Req_Created_Date,
#                    'Req_Descr': row.Req_Descr,
#                    'WO_Num': row.WO_Num,
#                    'PO_No': row.PO_No,
#                    'Buyer': row.Buyer,
#                    'HOLD_STATUS': row.HOLD_STATUS,
#                    'Out_to_bid': row.Out_to_bid,
#                    'Req_Status': row.Req_Status,
#                    'Req_Approval_Date': Req_Approval_Date,
#                    'Req_Total': row.Req_Total,
#                    'Req_Dflt_Tble_Buyer': row.Req_Dflt_Tble_Buyer,
#                    'PO_Date': PO_Date,
#                    'Date_Approved': Date_Approved,
#                }
#            }, upsert=True)
#            #uniquePOIDs[row.PO_No] = True

            db.EARLY_WARNING.insert(
                    {

                    'Req_Descr': row.Req_Descr,   
                    'Business_Unit': row.Unit,
                    'Req_ID': row.Req_ID,
                    'Req_Created_Date': Req_Created_Date,
                    'Req_Descr': row.Req_Descr,
                    'WO_Num': row.WO_Num,
                    'PO_No': row.PO_No,
                    'Buyer': row.Buyer,
                    'HOLD_STATUS': row.HOLD_STATUS,
                    'Out_to_bid': row.Out_to_bid,
                    'Req_Status': row.Req_Status,
                    'Req_Approval_Date': Req_Approval_Date,
                    'Req_Total': row.Req_Total,
                    'Req_Dflt_Tble_Buyer': row.Req_Dflt_Tble_Buyer,
                    'PO_Date': PO_Date,
                    'Date_Approved': Date_Approved,
                    })

    
    db.LAST_UPDATED.update({'dbname': "EARLY_WARNING"}, {'$set': {'last_updated_time': time.time()}})
    
    print("--- EARLY WARNING PULL COMPLETE REJOICE ---")
