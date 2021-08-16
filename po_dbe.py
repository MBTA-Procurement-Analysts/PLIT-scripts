# po_dbe.py
# Created by Mickey G for Project Tape Use.
# Upstream project is Monthly Report
# Oh boy

from datetime import datetime
from plitmongo import Lake
import sys

lake = Lake()
datestring, db_type = lake.parse_args(sys.argv[1:])
dbclient = lake.get_db(use_auth=False)
df = lake.get_df("po_dbe", "PLIT_MTHRPT_PO_DBE", datestring)
db_names = lake.get_db_names(db_type)

# PO_DBE_Status field of this raw query file has "NA", "??", and "Applicable",
#   and pandas will parse "NA" (string) as NA (concept of Null). So we first
#   replace NAs of that column to a string, then we will replace "??" too.
na_table = {"PO_DBE_Status": "Not Applicable"}
df = df.fillna(na_table)
df = df.replace({"PO_DBE_Status": {"??": "Undetermined"}})

# Selected needed columns, vouchers and PO Lines are dropped since we only want
#   DBE related info for PO (Keyed by Seq Num).
df = df[["Unit", "PO_No", "Vendor", "Name", "DBE_Goal", "PO_DBE_Status", "PO_Amt", "PO_Date",
         "Buyer", "Status", "Last_Dttm", "DBE_Attained", "SeqNum", "Type", "DBE_Percentage", "DBE_Status"]]
df = df.drop_duplicates()


data_time = datetime.now()

for db_name in db_names:
    db = dbclient[db_name]
    seen_po_dict = dict((po_no, False)
                        for po_no in df['PO_No'].unique().tolist())
    for row in df.itertuples():
        if not seen_po_dict[row.PO_No]:
            # Use the existece of 'Buyer' field to test whether the PO exist in
            #   the collection, since this script might already have created a
            #   PO object with DBE information.
            if db.PO_DATA.find({'$and': [{"PO_No": row.PO_No}, {"Buyer": {'$exists': True}}]}).count() == 0:
                lake._log("PO {} either does not exist, or is a DBE-only PO.".format(row.PO_No))
                lake._log(".. A DBE-only PO will be created if this PO does not exist.")
            db.PO_DATA.update({'$and': [{"Business_Unit": row.Unit}, {"PO_No": row.PO_No}]},
                                     {'$set': {
                                         "DBE_Goal": row.DBE_Goal,
                                         "DBE_Status": row.PO_DBE_Status,
                                         "DBE_Details": []
                                     }}, upsert=True)
            seen_po_dict[row.PO_No] = True

    for row in df.itertuples():
        db.PO_DATA.update({'$and': [{"Business_Unit": row.Unit}, {"PO_No": row.PO_No}]},
                                 {'$addToSet': {"DBE_Details":
                                                {"Vendor_Name": row.Vendor,
                                                 "Vendor_ID": row.Name,
                                                 "DBE_Attained": row.DBE_Attained,
                                                 "DBE_Sequence_No": row.SeqNum,
                                                 "DBE_Type": row.Type,
                                                 "DBE_Percentage": row.DBE_Percentage,
                                                 "DBE_Status": row.DBE_Status}}}, upsert=True)
    db.LAST_UPDATED.update({'dbname': "PO_DATA/DBE"}, 
    {'$set': {'last_updated_time': data_time}}, upsert=True)

lake.end()