# poMissingDataInsertion.py
# Created by Christopher M. for Project flextape use
# To Import PO data to mongodb, within the flextape pipeline

# On 4/2/2019, the script was created in order to automate emails for missing PO's

import sys
import time
import pandas as pd
from plitmongo import Lake

lake = Lake()
datestring, db_type = lake.parse_args(sys.argv[1:])
db_client = lake.get_db(use_auth=False)
df = lake.get_df("po-status-vendors", "CM_PO_OPN_BY_BU_BY_DATE_FS", datestring)
db_names = lake.get_db_names(db_type)

for db_name in db_names:
    db = db_client[db_name]
    uniquePOIDs = dict((pono, False) for pono in df['PO_No'].unique().tolist())
    for row in df.itertuples():
        if not uniquePOIDs[row.PO_No]:
            db.MISSING_PO.update({'PO_No': row.PO_No}, {
                '$set': {
                    "Vendor": row.Vendor,
                }
            }, upsert=True)
            uniquePOIDs[row.PO_No] = True

    db.LAST_UPDATED.update({'dbname': "PO_DATA"},
                           {'$set': {'last_updated_time': time.time()}},
                           upsert=True)

lake.end()
