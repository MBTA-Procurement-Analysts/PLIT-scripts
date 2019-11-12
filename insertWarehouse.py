# insertWarehouse.py
# Created by Nate O, modified by Mickey G for Project flextape use
# To Import Item warehouse data to mongodb, within the flextape pipeline

import sys
import time
from datetime import datetime
import pandas as pd
from plitmongo import Lake

lake = Lake()
datestring, db_type = lake.parse_args(sys.argv[1:])
db_client = lake.get_db(use_auth=False)
df = lake.get_df("itemwarehouse", "ITEM-WAREHOUSE", datestring)
db_names = lake.get_db_names(db_type)

na_table = {"Last_Ptwy": datetime(2001, 1, 1, 0, 0, 0)}

df = df.fillna(value=na_table)

for db_name in db_names:
    uniqueItemArr = df['Item'].unique().tolist()
    db = db_client[db_name]
    for itemno in uniqueItemArr:
        db.ITEM_DATA.update({'Item_No': itemno}, {
            '$set': {
                'Warehouse_Information': []
            }
        })

    for row in df.itertuples():
        db.ITEM_DATA.update({'Item_No': row.Item}, {
            '$push': {
                'Warehouse_Information': {
                    'Unit': row.Unit,
                    'Status_Current': row.Status_Current,
                    'Util_Type': row.Util_Type,
                    'Qty_On_Hand': row.Qty_On_Hand,
                    'Qty_Available': row.Qty_Avail,
                    'Reorder_Point': row.Reorder_Pt,
                    'Max_Qty': row.Max_Qty,
                    'Last_Month_Demand': row.Last_Mo,
                    'Last_Quarter_Demand': row.Last_Qtr,
                    'Last_Annual_Demand': row.Last_Ann,
                    'Avg_Price': row.Ave_Cost,
                    'Last_Putaway': row.Last_Ptwy,
                    'No_Replenish': row.No_Repl,
                    'Replenish_Class': row.Replen_Cls,
                    'Cost_Element': row.Cost_Elmnt,
                    'Replenish_Lead': row.Repln_Lead}
            }
        })
    db.LAST_UPDATED.update({'dbname': "ITEM_DATA"},
                           {'$set': {'last_updated_time': time.time()}},
                           upsert=True)

lake.end()
