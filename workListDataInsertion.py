# poDataInsertion.py
# Created by Christopher M for Project flextape use
# To Import PO data to mongodb, within the flextape pipeline

# On 06/28/2019, the script was modified to accomodate the addition
#   of 'PO Approval Date' field.

# ##########################################################################
# This script is not being used as of 2019-10-25, and a newer set named
#   `approval_workflow` has all the functions of this current set, thus
#   this script is being left as is, without adaptations to the October 2019
#   v2 template.
# Corresponding selenium script is `selenium-worklist-po-6.py`;
# Corresponding tape script is `tape-worklist.sh`.
# Both selenium and tape scripts has been soft disabled.
# ##########################################################################

import pandas as pd
import pymongo
from pymongo import MongoClient
from tqdm import tqdm
import sys
import time
import os

print("poDataInsertion.py taking over...")

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

filepath = filepathprefix + "pow/" + date + "/SLT_PO_WF_" + date + ".xlsx"

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
    uniquePOIDs = dict((pono, False)
                       for pono in insertionItems['PO_No'].unique().tolist())
    for row in tqdm(insertionItems.itertuples()):
        if not uniquePOIDs[row.PO_No]:

            if pd.isna(row.Denial_Date_Time):
                DDT = ""
            else:
                DDT = row.Denial_Date_Time

            if pd.isna(row.Dispatch_DTTM):
                Dispatch_Date = ""
            else:
                Dispatch_Date = row.Dispatch_DTTM

            if pd.isna(row.PO_Hdr_Created_Date):
                Created_Date = ""
            else:
                Created_Date = row.PO_Hdr_Created_Date

            db.PO_DATA.update({'PO_No': row.PO_No}, {
                '$set': {
                    'PO_No': row.PO_No,
                    "worklist": []
                }
            }, upsert=True)
            uniquePOIDs[row.PO_No] = True

    for row in tqdm(insertionItems.itertuples()):
        db.PO_DATA.update({"PO_No": row.PO_No}, {
            '$push': {
                "worklist": {
                    "Appr_Inst": row.Appr_Inst,
                    "Work_List": row.Work_List,
                    "Approval_Number": row.Approval_Number,
                    "Appr_Stat": row.Appr_Stat,
                    "Denial_Date_Time": DDT,
                    "Lv_Appr": row.User,
                    "Unit": row.Unit,
                    "PO_HDR_Status": row.PO_HDR_Status,
                    "WF_APPR_Status": row.WF_APPR_Status,
                    "Dispatch_DTTM": Dispatch_Date,
                    "PO_Hdr_Created_Date": Created_Date,
                    "SUM_MERCHANDISE_AMT": row.SUM_MERCHANDISE_AMT,
                },
            }
        })

    db.LAST_UPDATED.update({'dbname': "PO_DATA"}, {
                           '$set': {'last_updated_time': time.time()}})

    print("--- PO Update Done ---")
