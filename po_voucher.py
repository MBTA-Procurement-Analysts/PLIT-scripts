# po_voucher.py
# Created by Mickey G for Project Tape Use.
# Upstream project is Monthly Report, but voucher data can be used for other
#   purposes, thus the filename does not have the "monthlyreport" prefix.

#%%
cd /home/rubix/Desktop/Project-Ducttape/scripts

#%%
import sys
from plitmongo import Lake
from datetime import datetime

lake = Lake()
# datestring, db_type = lake.parse_args(sys.argv[1:])
datestring, db_type = lake.parse_args(["10012019-165914", "dev"])
dbclient = lake.get_db(use_auth=False)
df = lake.get_df("po_voucher", "PLIT_PO_LINE_DISTRIB_VOUCHER_F", datestring)
db_names = lake.get_db_names(db_type)

#%%
# This Dataframe does not have NA values. (Dream)

data_time = datetime.now()

for db_name in db_names:
    db = dbclient[db_name]
    for row in df.itertuples():
        db.VOUCHER_DATA.update_one({'$and': [{"Business_Unit": row.Unit},
                                             {"PO_No": row.PO_No},
                                             {"Line": row.Line},
                                             {"Schedule": row.Sched_Num},
                                             {"Distribution": row.Distribution_Li},
                                             {"Voucher_No": row.Voucher},
                                             {"Voucher_Line": row.Voucher_Line},
                                             {"Voucher_Amount": row.Amount},
                                             {"Account": row.Account},
                                             {"Accounting_Date": row.Acctg_Date}]},
                                   {'$set': {
                                       "rubix_SchemaVersion": "20190924",
                                       "rubix_DataTimestamp": data_time
                                   }}, upsert=True)

    db.LAST_UPDATED.update({'dbname': "VOUCHER_DATA"}, {
                           'set': {'last_updated_time': data_time}})
lake.end()
