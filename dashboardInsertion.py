# dashboardInsertion.py
# Created by Nate O, modified by Mickey G for Project flextape use
# To Import Req data to mongodb, within the flextape pipeline

import sys
import time
import pandas as pd
from datetime import datetime
from plitmongo import Lake

lake = Lake()
datestring, db_type = lake.parse_args(sys.argv[1:])
db_client = lake.get_db(use_auth=False)
df = lake.get_df("worklist", "NO_WORKLIST", datestring)
db_names = lake.get_db_names(db_type)

na_table = {"Transmitted_Time": datetime(2001, 1, 1, 0, 0, 0)}

df = df.fillna(value=na_table)

for db_name in db_names:
    db = db_client[db_name]
    # We drop the collection everytime we refresh the dashboard data, since
    #   the upstream query is a snapshot and records come and go in the query.
    # There will be no way to detect if the record is not in the query anymore
    #   than removing anything that is not in the current query; and dropping
    #   the entire collection and then adds the current items is faster.
    db.DASHBOARD_DATA.drop()
    for row in df.itertuples():
        db.DASHBOARD_DATA.update({"REQ_No": row.Requisition_ID},
                                 {"$set": {
                                     'Req_ID': row.Requisition_ID,
                                     'Business_Unit': row.Business_Unit,
                                     'Buyer': row.Buyer,
                                     'Hold_From_Further_Processing': row.Hold_From_Further_Processing,
                                     'Hold_Status': row.Hold_Status,
                                     'Sourcing': row.Sourcing,
                                     'Lines_Not_Sourced': row.Nbr_Lines_Not_Sourced,
                                     'Out_To_Bid': row.Out_To_Bid,
                                     'Transmitted': row.Transmitted,
                                     'Transmitted_Time': row.Transmitted_Time
                                 }}, upsert=True)

    db.LAST_UPDATED.update({'dbname': "DASHBOARD_DATA"},
                           {'$set': {'last_updated_time': time.time()}},
                           upsert=True)

lake.end()
