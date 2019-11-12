# earlyWarningDataInsertion.py
# Created by Christopher M
# To Import PO data to mongodb, within the flextape pipeline

import sys
import time
import pandas as pd
from datetime import datetime
from plitmongo import Lake

lake = Lake()
datestring, db_type = lake.parse_args(sys.argv[1:])
db_client = lake.get_db(use_auth=False)
df = lake.get_df("early_warning", "SLT_CPTL_EARLYWARNING", datestring)
db_names = lake.get_db_names(db_type)

dummy_date = datetime(2001, 1, 1, 0, 0, 0)

na_table = {"Date": dummy_date,
            "PO_Date": dummy_date,
            "Req_Approval_Date": dummy_date,
            "Req_Created_Date": dummy_date}

df = df.fillna(value=na_table)

for db_name in db_names:
    db = db_client[db_name]
    db.EARLY_WARNING.drop()
    for row in df.itertuples():
        db.EARLY_WARNING.insert(
            {
                'Req_Descr': row.Req_Descr,
                'Business_Unit': row.Unit,
                'Req_ID': row.Req_ID,
                'Req_Created_Date': row.Req_Created_Date,
                'Req_Descr': row.Req_Descr,
                'WO_Num': row.WO_Num,
                'PO_No': row.PO_No,
                'Buyer': row.Buyer,
                'HOLD_STATUS': row.HOLD_STATUS,
                'Out_to_bid': row.Out_to_bid,
                'Req_Status': row.Req_Status,
                'Req_Approval_Date': row.Req_Approval_Date,
                'Req_Total': row.Req_Total,
                'Req_Dflt_Tble_Buyer': row.Req_Dflt_Tble_Buyer,
                'PO_Date': row.PO_Date,
                'Date_Approved': row.Date,
            })

    db.LAST_UPDATED.update({'dbname': "EARLY_WARNING"},
                           {'$set': {'last_updated_time': time.time()}},
                           upsert=True)

lake.end()
