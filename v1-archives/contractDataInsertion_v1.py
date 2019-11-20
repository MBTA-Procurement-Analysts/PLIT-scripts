# contractDataInsertion.py
# Created by Nate O, modified by Mickey G for Project flextape use
# To Import Req data to mongodb, within the flextape pipeline

import pandas as pd
import pymongo
from pymongo import MongoClient
from tqdm import tqdm
import sys
import time
from bson import son
from bson.codec_options import CodecOptions
from collections import OrderedDict
import os

print("contractDataInsertion.py taking over...")

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

filepath = filepathprefix + "contract/" + date + "/NO_CONTRACT_RUBIX-" + date + ".xlsx"

print("--- Reading " + filepath + " ---")

insertionItems = pd.read_excel(filepath, skiprows = 1)

client = MongoClient()

insertionItems.columns = [c.replace(' ', '_') for c in insertionItems.columns]
insertionItems.columns = [c.replace('/', '_') for c in insertionItems.columns]
insertionItems.columns = [c.replace('.', '') for c in insertionItems.columns]


for location in writelocation:
    uniqueContractIDs = dict((contractNo, False) for contractNo in insertionItems['Contract'].unique().tolist())
    dbname = 'rubix-' + serverlocation + '-' + location
    print('Using database ' + dbname)
    db = client[dbname]
    for row in tqdm(insertionItems.itertuples()):
        if not uniqueContractIDs[row.Contract]:
            if pd.isna(row.Expire_Dt):
                exprDate = ""
            else:
                exprDate = row.Expire_Dt
            db.CONTRACT_DATA.update_one({'Contract_ID': row.Contract}, {
                '$set': {
                    'Contract_ID': row.Contract,
                    'Expire_Date': exprDate,
                    'Vendor_ID': row.Vendor,
                    "Vendor_Name": row.Name,
                    "lines": []
                    }
                }, upsert=True)
        uniqueContractIDs[row.Contract] = True
    for row in tqdm(insertionItems.itertuples()):
        db.CONTRACT_DATA.update_one({"Contract_ID": row.Contract}, {
            '$addToSet': {
                'lines': OrderedDict({
                    "line": row.Line,
                    "More_Info": row.More_Info,
                    "Max_Amt": row.Max_Amt})
                }}, upsert = True)



    db.LAST_UPDATED.update({'dbname': "CONTRACT_DATA"}, {'$set': {'last_updated_time': time.time()}})
    print("Contract Update Done!")
