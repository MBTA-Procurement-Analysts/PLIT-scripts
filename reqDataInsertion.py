# reqDataInsertion.py
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
df = lake.get_df("req-6", "PLIT_REQ_6", datestring)
db_names = lake.get_db_names(db_type)

# TOTEST

# It is possible for a REQ to have no Buyer associated with it.
# One such case is that when a REQ was "federalized" (i.e. change of biz unit)
# The original REQ will lose it's Requester and Buyer, making these fields NAs
#   in Pandas, and thus `NaN`s in mongodb.
na_table = {"Due": datetime(2001, 1, 1, 0, 0, 0),
            "Origin": "",
            "By": "",
            "Name": "",
            "More_Info": "",
            "Mfg_ID": "",
            "Address_1": "",
            "Address_2": "",
            "City": "",
            "St": "",
            "Postal": "",
            "Item": "",
            "Buyer": "",
            "Requester": ""}
df = df.fillna(value=na_table)

for db_name in db_names:
    uniqueReqIDs = dict((reqno, False)
                        for reqno in df['Req_ID'].unique().tolist())
    uniqueItemArr = df['Item'].unique().tolist()
    db = db_client[db_name]
    for row in df.itertuples():
        if not uniqueReqIDs[row.Req_ID]:
            db.REQ_DATA.update({'REQ_No': row.Req_ID}, {
                '$set': {
                    'REQ_No': row.Req_ID,
                    'Account': row.Account,
                    'Business_Unit': row.Unit,
                    'Buyer': row.Buyer,
                    'Currency': row.Currency,
                    'Department': {
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
                        "Zip_Code": "{:0>5}".format(row.Postal),
                        "Country": row.Cntry
                    },
                    "Status": row.Status,
                    "Approved_By": row.By,
                    "Approved_On": row.Date,
                    "Vendor": {
                        "Number": row.Vendor,
                        "Name": row.Name
                    },
                    "lines": []
                }
            }, upsert=True)
            uniqueReqIDs[row.Req_ID] = True

    for row in df.itertuples():
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

    db.LAST_UPDATED.update({'dbname': "REQ_DATA"}, {
                           '$set': {'last_updated_time': time.time()}}, upsert=True)

lake.end()
