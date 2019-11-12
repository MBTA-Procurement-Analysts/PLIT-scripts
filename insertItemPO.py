# insertItemPO.py
# Created by Nate O, modified by Mickey G for Project flextape use
# To Import Item historical PO to mongodb, within the flextape pipeline

import sys
import time
import pandas as pd
from plitmongo import Lake

lake = Lake()
datestring, db_type = lake.parse_args(sys.argv[1:])
db_client = lake.get_db(use_auth=False)
df = lake.get_df("itempo", "ITEM-PO", datestring)
db_names = lake.get_db_names(db_type)

for db_name in db_names:
    # Find unique Item IDs, since Items with multiple PO listings spans
    #   across multiple lines.
    # Dictionary is not necessary since we do not need to update any data for just
    #   once, and thus we don't need to check if we are writing multiple times for
    #   the same Item ID.
    uniqueItemArr = df['Item'].unique().tolist()
    db = db_client[db_name]

    for itemno in uniqueItemArr:
        db.ITEM_DATA.update({'Item_No': itemno},
                            {'$set': {'Previous_PO': []}}, upsert=True)

    for row in df.itertuples():
        db.ITEM_DATA.update({'Item_No': row.Item},
                            {'$push':
                             {'Previous_PO':
                              {'PO_No': row.PO_No,
                               'Name': row.Name,
                               'PO_Date': row.PO_Date,
                               'PO_Status': row.Status,
                               'DistribStatus': row.PO_Line_Status,
                               'ReceiptStatus': row.Receipt,
                               'PO_Qty': row.PO_Qty,
                               'Received_Qty': row.Received_Qty,
                               'Amount': row.Amount}}})

    db.LAST_UPDATED.update({'dbname': "ITEM_DATA"},
                           {'$set': {'last_updated_time': time.time()}},
                           upsert=True)

lake.end()
