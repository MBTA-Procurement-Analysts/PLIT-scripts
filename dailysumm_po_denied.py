import sys
import time
import pandas as pd
from plitmongo import Lake
import datetime as datetime

lake = Lake()
datestring, db_type = lake.parse_args(sys.argv[1:])
db_client = lake.get_db(use_auth = False)
df = lake.get_df("daily_summ_po_denied", "DAILY-PO-DENIED", datestring)
db_names = lake.get_db_names(db_type)

na_table = {"Appr_Act":""}

df = df.fillna(value = na_table)
pulling_date = datetime.datetime.now().replace(hour=0, minute=0, microsecond=0)
for db_name in db_names:
    uniquePOIDs = dict((pono, False) for pono in df['PO_No'].unique().tolist())
    db = db_client[db_name]
    for row in df.itertuples():
        if not uniquePOIDs[row.PO_No]:
            db.DAILY_SUMM_PO_DENIED.update({'PO_No': row.PO_No}, {
                '$set': {
                    'Unit': row.Unit,
                    'PO_No': row.PO_No,
                    'Description': row.Descr,
                    'PO_HDR_Status': row.PO_HDR_Status,
                    'PO_Date': row.PO_Date,
                    'Buyer': row.Buyer,
                    'PO_APPR_WF_Status': row.PO_APPR_WF_Status,
                    'Appr_Act': row.Appr_Act,
                    'Appr_Inst': row.Appr_Inst,
                    'Vendor_Name': row.Vendor_Name,
                    'Pulling_Date': pulling_date
            }
        }, upsert = True)
        uniquePOIDs[row.PO_No] = True
    db.LAST_UPDATED.update({'dbname': "DAILY_SUMM_PO_DENIED"}, {
        '$set': {'last_updated_time': datetime.datetime.now()}
    }, upsert = True)

lake.end()
