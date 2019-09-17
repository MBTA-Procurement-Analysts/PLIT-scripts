# contract_plit.py
# Created by Mickey G for Project Tape Use.

import sys
from plitmongo import Lake
from datetime import datetime
import progressbar
import time

lake = Lake()
datestring, db_type = lake.parse_args(sys.argv[1:])
dbclient = lake.get_db(use_auth=False)
df = lake.get_df("contract_plit", "JLYU_CONTRACT_PUBLIC", datestring)
db_names = lake.get_db_names(db_type)


na_table = {
    "Contract": "",
    "Vendor": "",
    "Status": "",
    "By": "",
    "Date": datetime(1990, 1, 1, 0, 0, 0),
    "Type": "",
    "Descr": "",
    "Begin_Dt": datetime(1990, 1, 1, 0, 0, 0),
    "Expire_Dt": datetime(1990, 1, 1, 0, 0, 0),
    "Max_Amt": 0,
    "Buyer": "",
    "AMT_Relsd": 0,
    "Name": "",
    "Entered": datetime(1990, 1, 1, 0, 0, 0),
    "Entered_By": "",
    "Days_Notify": 0,
    "ContractCat": ""}

df = df.fillna(value=na_table)

data_time = datetime.now()

for db_name in db_names:
    db = dbclient[db_name]
    for row in df.itertuples():
        db.CONTRACT_PLIT_FMIS.update({"CONTRACT_No": row.Contract},
                                     {'$set': {
                                         "CONTRACT_No": row.Contract,
                                         "Vendor": row.Vendor,
                                         "Status": row.Status,
                                         "Approved_By": row.By,
                                         "Approved_Date": row.Date,
                                         "Contract_Type": row.Type,
                                         "Description": row.Descr,
                                         "Begin_Date": row.Begin_Dt,
                                         "Expire_Date": row.Expire_Dt,
                                         "Max_Amount": row.Max_Amt,
                                         "Buyer": row.Buyer,
                                         "Amount_Release": row.AMT_Relsd,
                                         "Vendor_Name": row.Name,
                                         "Entered_Date": row.Entered,
                                         "Entered_By": row.Entered_By,
                                         "Notify_Days_Prior": row.Days_Notify,
                                         "Contract_Category": row.ContractCat,
                                         "rubix_SchemaVersion": "20190916",
                                         "rubix_DataTimestamp": data_time
                                     }}, upsert=True)

    db.LAST_UPDATED.update({'dbname': "CONTRACT_PLIT_FMIS"}, {
                           '$set': {'last_updated_time': data_time}})
