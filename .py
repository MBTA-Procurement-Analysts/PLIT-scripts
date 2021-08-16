# .py
# Created by 
# 

import sys
import time
import pandas as pd
from plitmongo import Lake

lake = Lake()
datestring, db_type = lake.parse_args(sys.argv[1:])
db_client = lake.get_db(use_auth=False)
df = lake.get_df("", "", datestring)
db_names = lake.get_db_names(db_type)

# NA handling
na_table = {}

df = df.fillna(value=na_table)

for db_name in db_names:
    db = db_client[db_name]
    
    for row in df.itertuples():

    db.LAST_UPDATED.update({'dbname': CHANGE_ME}, {
                           '$set': {'last_updated_time': time.time()}}, upsert=True)

lake.end()