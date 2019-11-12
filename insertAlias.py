# insertAlias.py
# Created by Nate O, modified by Mickey G for Project flextape use
# To Import Item metadata to mongodb, within the flextape pipeline

import sys
import time
import pandas as pd
from plitmongo import Lake

lake = Lake()
datestring, db_type = lake.parse_args(sys.argv[1:])
db_client = lake.get_db(use_auth=False)
df = lake.get_df("item_alias", "ITEM-ALIAS", datestring)
db_names = lake.get_db_names(db_type)

for db_name in db_names:
    # Find unique Item IDs, since Items with multiple viable substitues spans across
    #   multiple lines.
    uniqueItemIDs = dict((itemno, False)
                         for itemno in df['Item'].unique().tolist())
    db = db_client[db_name]

    for itemno, _ in uniqueItemIDs.items():
        db.ITEM_DATA.update({'Item_No': itemno}, {
            '$set': {
                'Viable_Subs': []
            }
        }, upsert=True)

    for row in df.itertuples():
        if not uniqueItemIDs[row.Item]:
            db.ITEM_DATA.update({'Item_No': row.Item}, {
                '$set': {
                    'Item_Description': row.Descr,
                    'Item_Group': {
                        "Group_Number": row.Item_Group,
                        "Group_Description": row.GDescr
                    },
                    'Status': row.Status_Current,
                    'UOM': row.Std_UOM,
                    'Status_Date': row.Itm_Status_Dt,
                }
            }, upsert=True)
            uniqueItemIDs[row.Item] = True

        db.ITEM_DATA.update({'Item_No': row.Item}, {
            '$push': {
                'Viable_Subs': {
                    'Mfg_ID': row.Mfg_ID,
                    'Mfg_Item_ID': row.Mfg_Itm_ID
                }
            }
        })
    db.LAST_UPDATED.update({'dbname': "ITEM_DATA"},
                           {'$set': {'last_updated_time': time.time()}},
                           upsert=True)

lake.end()
