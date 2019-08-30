# approvalWorkflow.py
# Created by Mickey G for Project Flextape use
# To import Approveal Workflow data to mongodb, with flextape pipeline


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

filepath = filepathprefix + "approval_workflow/" + date + "/SLT_PO_WF-" + date + ".xlsx"

print("--- Reading " + filepath + " ---")

# Reads Excel File
insertionItems = pd.read_excel(filepath, skiprows = 1)

# Fills spaces in Column Names with underscores
insertionItems.columns = [c.replace(' ', '_') for c in insertionItems.columns]
insertionItems.columns = [c.replace('.', '') for c in insertionItems.columns]

client = MongoClient()

print("--- Pushing Data to Mongo ---")

for location in writelocation:
    dbname = "rubix-{}-{}".format(serverlocation, location)
    print("Using database {}".format(dbname))
    db = client[dbname]
    # clear existing data
    uniquePOArr = insertionItems['PO_No'].unique().tolist()
    for po_num in uniquePOArr:
        db.PO_DATA.update_one({"PO_No": po_num}, {'$set': {"worklist": []}})

    for row in tqdm(insertionItems.itertuples()):
        db.PO_DATA.update_one({"PO_No": row.PO_No}, {'$push': {"worklist": {
                        "Appr_Inst": row.Appr_Inst,
                        "Work_List": row.Work_List if not pd.isna(row.Work_List) else "",
                        "Approval_Number": row.Approval_Number if not pd.isna(row.Approval_Number) else "",
                        "Appr_Stat": row.Appr_Stat if not pd.isna(row.Appr_Stat) else "",
                        "Event_Date_Time": row.Event_Date_Time,
                        "User": row.User,
                        "Unit": row.Unit,
                        "PO_HDR_Status": row.PO_HDR_Status,
                        "WF_APPR_Status": row.WF_APPR_Status,
                        "Dispatch_DTTM": row.Dispatch_DTTM,
                        "Threshold": row.Threshold,
                        "Buyer": row.Buyer,
                        "PO_Hdr_Created_Date": row.PO_Hdr_Created_Date,
                        "SUM_MERCHANDISE_AMT": row.SUM_MERCHANDISE_AMT
                    }}})

    db.LAST_UPDATED.update_one({'dbname': "PO_DATA_WORKFLOW"}, {'$set': {'last_updated_time': time.time()}})

print("--- PO Workflow Update Done ---")