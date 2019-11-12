# insertBackorder.py
# Created by Nate O, modified by Mickey G for Project flextape use
# To Import Item backorder metadata to mongodb, within the flextape pipeline

import sys
import time
import pandas as pd
from plitmongo import Lake

lake = Lake()
datestring, db_type = lake.parse_args(sys.argv[1:])
db_client = lake.get_db(use_auth=False)
df = lake.get_df("itembackorder", "ITEM-BACKORDER", datestring)
db_names = lake.get_db_names(db_type)

for db_name in db_names:
    db = db_client[db_name]
    # Resets all Backorder Amount that != 0 to 0, since the original query will
    #   not include 0-amount entries; thus if the amount goes to 0 there is no
    #   entry in the query.
    # Multi Flag set as true to cover all Entries
    db.ITEM_DATA.update({'Total_Backorder': {'$ne': 0}},
                        {'$set': {'Total_Backorder': 0}}, multi=True)

    for row in df.itertuples():
        db.ITEM_DATA.update({'Item_No': row.Item},
                            {'$set':
                             {'Total_Backorder': row.Qty_Req}})

    db.LAST_UPDATED.update({'dbname': "ITEM_DATA"},
                           {'$set': {'last_updated_time': time.time()}},
                           upsert=True)

lake.end()
