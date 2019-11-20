# insertWarehouse.py
# Created by Nate O, modified by Mickey G for Project flextape use
# To Import Item warehouse data to mongodb, within the flextape pipeline

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

filepath = filepathprefix + "itemwarehouse/" + date + "/ITEM-WAREHOUSE-" + date + ".xlsx"

insertionItems = pd.read_excel(filepath, skiprows = 1) 

client = MongoClient()
insertionItems.columns = [c.replace(' ', '_') for c in insertionItems.columns]

insertionItems['Item'] = insertionItems['Item'].str.strip()


for location in writelocation:
    uniqueItemArr = insertionItems['Item'].unique().tolist()
    dbname = 'rubix-' + serverlocation + '-' + location 
    print('Using database ' + dbname)
    db = client[dbname]
    for itemno in tqdm(uniqueItemArr):
        db.ITEM_DATA.update({'Item_No': itemno}, {
            '$set': {
                'Warehouse_Information': []
                }
            })
    
    for row in tqdm(insertionItems.itertuples()):
        if pd.isna(row.Last_Ptwy):
            ptawayDate = ""
        else:
            ptawayDate = row.Last_Ptwy
        db.ITEM_DATA.update({'Item_No': row.Item}, {
            '$push': {
                'Warehouse_Information': {
                    'Unit': row.Unit,
                    'Status_Current': row.Status_Current,
                    'Util_Type': row.Util_Type,
                    'Qty_On_Hand': row.Qty_On_Hand,
                    'Qty_Available': row.Qty_Avail,
                    'Reorder_Point':row.Reorder_Pt,
                    'Max_Qty': row.Max_Qty,
                    'Last_Month_Demand': row.Last_Mo,
                    'Last_Quarter_Demand': row.Last_Qtr,
                    'Last_Annual_Demand': row.Last_Ann,
                    'Avg_Price': row.Ave_Cost,
                    'Last_Putaway': ptawayDate,
                    'No_Replenish': row.No_Repl,
                    'Replenish_Class': row.Replen_Cls,
                    'Cost_Element': row.Cost_Elmnt,
                    'Replenish_Lead': row.Repln_Lead}
        }
        })
    db.LAST_UPDATED.update({'dbname': "ITEM_DATA"}, {'$set': {'last_updated_time': time.time()}})
        # db.REQ_DATA.find({"lines": {$elemMatch: {"Item": "02545903"}}})
