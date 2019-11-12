# poInReqLines.py
# Created by Mickey G for Project flextape use
# To Import PO # and Line number data to REQ , within the flextape pipeline

import sys
import time
import pandas as pd
from plitmongo import Lake

lake = Lake()
datestring, db_type = lake.parse_args(sys.argv[1:])
db_client = lake.get_db(use_auth=False)
df = lake.get_df("req_line_po_line", "PLIT_REQ_LINE_PO_LINE", datestring)
db_names = lake.get_db_names(db_type)

df = df.rename(
    columns={"Line": "REQ_Line", "Line1": "PO_Line"})

for db_name in db_names:
    db = db_client[db_name]
    for row in df.itertuples():
        db.REQ_DATA.update_one({"$and": [{"REQ_No": row.Req_ID},
                                         {"Business_Unit": row.Unit}]},
                               {"$set": {"lines.$[l].PO": {
                                   "PO_Number": row.PO_No,
                                   "Line_No": row.PO_Line}}},
                               array_filters=[{"l.Line_No": row.REQ_Line}])

    db.LAST_UPDATED.update({'dbname': "PO_DATA_IN_REQ"},
                           {'$set': {'last_updated_time': time.time()}},
                           upsert=True)

lake.end()
