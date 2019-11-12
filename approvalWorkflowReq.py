# approvalWorkflowReq.py
# Created by Mickey G for Project Flextape use
# To import Req Approveal Workflow data to mongodb, with flextape pipeline

import sys
import time
import pandas as pd
from plitmongo import Lake

lake = Lake()
datestring, db_type = lake.parse_args(sys.argv[1:])
db_client = lake.get_db(use_auth=False)
df = lake.get_df("approval_workflow_req", "SLT_REQ_WF", datestring)
db_names = lake.get_db_names(db_type)

na_table = {"Work_List": "",
            "Approval_Number": ""}

df = df.fillna(value=na_table)

for db_name in db_names:
    db = db_client[db_name]
    # clear existing data
    uniqueReqArr = df['Req_ID'].unique().tolist()
    for req_num in uniqueReqArr:
        db.REQ_DATA.update_one({"REQ_No": req_num}, {'$set': {"worklist": []}})

    for row in df.itertuples():
        db.REQ_DATA.update_one({"REQ_No": row.Req_ID}, {"$push": {"worklist": {
            "Appr_Inst": row.Appr_Inst,
            "Unit": row.Unit,
            "Appr_Step": row.Appr_Step,
            "Appr_Path": row.Appr_Path,
            "Work_List": row.Work_List if not pd.isna(row.Work_List) else "",
            "WL_Tran_ID": row.WL_Tran_ID,
            "Approval_Number": row.Approval_Number if not pd.isna(row.Approval_Number) else "",
            "Appr_Stat": row.Appr_Stat,
            "Status": row.Status,
            "Date_Time": row.Date_Time,
            "User": row.User,
            "Req_Date": row.Req_Date
        }}})

    db.LAST_UPDATED.update_one({'dbname': "REQ_DATA_WORKFLOW"}, {
                               '$set': {'last_updated_time': time.time()}},
                               upsert=True)

lake.end()
