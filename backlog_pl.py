# backlog_pl.py
# Created by Sam Delfino 
# Access Buyer Backlog Data
import warnings
warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")

import sys
import time
import pandas as pd
from plitmongo import Lake

lake = Lake()
datestring, db_type  = lake.parse_args(sys.argv[1:])
db_client = lake.get_db(use_auth=False)
df = lake.get_df("backlog_pl", "PL_BUYER_BACKLOG", datestring)
db_names = lake.get_db_names(db_type)


# NA handling
na_table = {}

df = df.fillna(value=na_table)

for db_name in db_names:
        db = db_client[db_name]

        for row in df.itertuples():

                    db.LAST_UPDATED.update({'dbname': 'PL_bUYER_BACKLOG'}, {
                                                       '$set': {'last_updated_time': time.time()}}, upsert=True)

lake.end()
