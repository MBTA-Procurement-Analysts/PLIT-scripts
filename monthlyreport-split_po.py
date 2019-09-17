# monthlyreport-split_po.py
# Created by Mickey G for Project Tape Use.
# Upstream project is Monthly Report

import sys
from plitmongo import Lake
from datetime import datetime

lake = Lake()
datestring, db_type = lake.parse_args(sys.argv[1:])
dbclient = lake.get_db(use_auth=False)
df = lake.get_df("split_po", "NG_NO_SPILT_PO_SIDE", datestring)
db_names = lake.get_db_names(db_type)

na_table = {"Origin": "",
            "QuoteLink": ""}

df = df.fillna(value=na_table)

data_time = datetime.now()

for db_name in db_names:
    db = dbclient[db_name]
    for row in df.itertuples():
        db.MTHRPT_PO.update({'$and': [{"Business_Unit": row.Business_Unit},
                                      {"PO_No": row.PO_No}]},
                            {'$set': {
                                "Business_Unit": row.Business_Unit,
                                "PO_No": row.PO_No,
                                "PO_Line": row.PO_Line,
                                "Status": row.Status,
                                "PO_Date": row.PO_Date,
                                "Buyer": row.Buyer,
                                "Origin": row.Origin,
                                "Sum_Amount": row.Sum_Amount,
                                "Vendor_Name": row.Vendor_Name,
                                "Descr": row.Descr,
                                "REQ_ID": row.Req_ID,
                                "QuoteLink": row.QuoteLink,
                                "rubix_SchemaVersion": "20190913",
                                "rubix_DataTimestamp": data_time
                            }}, upsert=True)

    db.LAST_UPDATED.update({'dbname': "MTHRPT_PO"}, {'$set': {'last_updated_time': data_time}})
