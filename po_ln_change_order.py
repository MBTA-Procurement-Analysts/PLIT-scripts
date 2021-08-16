# po_ln_change_order.py
# Created by Nate O, modified by Mickey G for Project flextape use
# To Import PO data to mongodb, within the flextape pipeline

import sys
import time
import pandas as pd
from plitmongo import Lake

lake = Lake()
datestring, db_type = lake.parse_args(sys.argv[1:])
db_client = lake.get_db(use_auth=False)
df = lake.get_df("po_ln_change_order", "PLIT_PO_LN_CHANGE_ORDER", datestring)
db_names = lake.get_db_names(db_type)

exit()
# lines.Requisition_Data NA handling
na_table = {"Req_ID": "",
            "REQ_Line": 0}

df = df.fillna(value=na_table)

for db_name in db_names:
    db = db_client[db_name]
    uniquePOIDs = dict((pono, False) for pono in df['PO_No'].unique().tolist())
    for row in df.itertuples():
        if not uniquePOIDs[row.PO_No]:
            if pd.isna(row.PO_AprvDate):
                aprvDate = ""
            else:
                aprvDate = row.PO_AprvDate
            db.PO_DATA.update({'PO_No': row.PO_No}, {
                '$set': {
                    'PO_No': row.PO_No,
                    'Business_Unit': row.Business_Unit,
                    'Buyer': row.Buyer,
                    "Status": row.Status,
                    'PO_Date': row.PO_Date,
                    "Origin": row.Origin,
                    "Approved_By": row.Approved_By,
                    "Vendor": row.Vendor_Name,
                    "PO_Approval_Date": aprvDate,
                    "Last_Updated": row.Last_Dttm,
                    "Vendor_ID": row.Vendor,
                    "lines": []
                }
            }, upsert=True)
            uniquePOIDs[row.PO_No] = True

    for row in df.itertuples():
        db.PO_DATA.update({"PO_No": row.PO_No}, {
            '$push': {
                "lines": {
                    "Line_No": row.PO_Line,
                    "Mfg_ID": row.Mfg_ID,
                    "Mfg_Item_ID": row.Mfg_Itm_ID,
                    "Line_Total": row.Sum_Amount,
                    "Level_1": row.Level_1,
                    "Level_2": row.Level_2,
                    "Descr": row.Descr,
                    "Line_Descr": row.More_Info,
                    "FairMarkIt_Link": row.QuoteLink,
                    "Requisition_Data": {
                        "Req_ID": row.Req_ID,
                        "Req_Line": row.REQ_Line
                    },
                    "Quantity": row.PO_Qty
                }
            }})

    db.LAST_UPDATED.update({'dbname': "PO_DATA"}, {
                           '$set': {'last_updated_time': time.time()}}, upsert=True)

lake.end()
