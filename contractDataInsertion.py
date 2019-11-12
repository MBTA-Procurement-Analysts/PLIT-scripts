# contractDataInsertion.py
# Created by Nate O, modified by Mickey G for Project flextape use
# To Import Req data to mongodb, within the flextape pipeline

import sys
import time
from datetime import datetime
import pandas as pd
from plitmongo import Lake

lake = Lake()
datestring, db_type = lake.parse_args(sys.argv[1:])
db_client = lake.get_db(use_auth=False)
df = lake.get_df("contract", "NO_CONTRACT_RUBIX", datestring)
db_names = lake.get_db_names(db_type)

na_table = {"Expire_Dt": datetime(2001, 1, 1, 0, 0, 0)}

df = df.fillna(na_table)

for db_name in db_names:
    uniqueContractIDs = dict((contractNo, False)
                             for contractNo in df['Contract'].unique().tolist())
    db = db_client[db_name]
    for row in df.itertuples():
        if not uniqueContractIDs[row.Contract]:
            db.CONTRACT_DATA.update_one({'Contract_ID': row.Contract}, {
                '$set': {
                    'Contract_ID': row.Contract,
                    'Expire_Date': row.Expire_Dt,
                    'Vendor_ID': row.Vendor,
                    "Vendor_Name": row.Name,
                    "lines": []
                }
            }, upsert=True)
        uniqueContractIDs[row.Contract] = True
    for row in df.itertuples():
        db.CONTRACT_DATA.update_one({"Contract_ID": row.Contract}, {
            '$addToSet': {
                'lines': {
                    "line": row.Line,
                    "More_Info": row.More_Info,
                    "Max_Amt": row.Max_Amt}
            }}, upsert=True)

    db.LAST_UPDATED.update({'dbname': "CONTRACT_DATA"},
                           {'$set': {'last_updated_time': time.time()}},
                           upsert=True)

lake.end()
