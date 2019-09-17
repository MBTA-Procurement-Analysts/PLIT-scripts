# monthlyreport-timing.py
# Created by Mickey G for Project Tape Use.
# Upstream project is Monthly Report.

import sys
from plitmongo import Lake
from datetime import datetime
import progressbar

lake = Lake()
datestring, db_type = lake.parse_args(sys.argv[1:])
dbclient = lake.get_db(use_auth=False)
df = lake.get_df("timing", "MTHRPT_TIMING", datestring)
db_names = lake.get_db_names(db_type)

na_table = {"Date": datetime(2000,1,1,0,0,0),
            "Origin": ""}

df = df.fillna(value=na_table)

data_time = datetime.now()
for db_name in db_names:
    db = dbclient[db_name]
    for row in df.itertuples():
        db.MTHRPT_TIMING.update({"$and": [{"Business_Unit": row.Business_Unit}, 
                                          {"REQ_No": row.Req_ID}]}, 
                                {'$set': {
                                    "Business_Unit": row.Business_Unit,
                                    "REQ_No": row.Req_ID,
                                    "Buyer": row.Buyer,
                                    "Req_Date": row.Req_Date,
                                    "Approval_Date": row.Approval_Date,
                                    "PO_No": row.PO_No,
                                    "PO_Date": row.PO_Date,
                                    "PO_Approval_Date": row.PO_Date,
                                    "Last_Dttm": row.Last_Dttm,
                                    "Last_Activ": row.Last_Activ,
                                    "Origin": row.Origin,
                                    "rubix_SchemaVersion": "20190910",
                                    "rubix_DataTimestamp": data_time
                                }}, upsert=True)

    db.LAST_UPDATED.update({'dbname': "MTHRPT_TIMING"}, {'$set': {'last_updated_time': data_time}})