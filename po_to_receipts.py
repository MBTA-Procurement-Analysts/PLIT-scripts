# po_to_receipts.py
# Created by Sam Delfino
# Pulling Received PO Line Items Data
import warnings
warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")

import sys
import time
import pandas as pd
from plitmongo import Lake

lake = Lake()
datestring, db_type = lake.parse_args(sys.argv[1:])
db_client = lake.get_db(use_auth=False)
df = lake.get_df("po_to_receipts", "PL_PO_TO_RECEIPTS_PT1", datestring)
db_names = lake.get_db_names(db_type)

# NA handling
na_table = {}

df = df.fillna(value=na_table)

for db_name in db_names:
    db = db_client[db_name]
    
    for row in df.itertuples():

        db.LAST_UPDATED.update({'dbname': "PL_PO_TO_RECEIPTS_PT1"}, {
                           '$set': {'last_updated_time': time.time()}}, upsert=True)

lake.end()
