# approvalWorkflow.py
# Created by Mickey G for Project Flextape use
# To import Approveal Workflow data to mongodb, with flextape pipeline

import sys
import time
import pandas as pd
from plitmongo import Lake

lake = Lake()
datestring, db_type = lake.parse_args(sys.argv[1:])
db_client = lake.get_db(use_auth=False)
df = lake.get_df("approval_workflow", "PLIT_PO_WF", datestring)
db_names = lake.get_db_names(db_type)

na_table = {"Work_List": "",
            "Approval_Number": "",
            "Appr_Stat": ""}

df = df.fillna(value=na_table)

for db_name in db_names:
    db = db_client[db_name]
    uniquePOArr = df['PO_No'].unique().tolist()
    for po_num in uniquePOArr:
        db.PO_DATA.update_one({"PO_No": po_num}, {'$set': {"worklist": []}})

    for row in df.itertuples():
        db.PO_DATA.update_one({"PO_No": row.PO_No}, {'$push': {"worklist": {
            "Appr_Inst": row.Appr_Inst,
            "Work_List": row.Work_List,
            "Approval_Number": row.Approval_Number,
            "Appr_Stat": row.Appr_Stat,
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

    db.LAST_UPDATED.update_one({'dbname': "PO_DATA_WORKFLOW"},
                               {'$set': {'last_updated_time': time.time()}},
                               upsert=True)

lake.end()
