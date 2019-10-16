# timeline_req_hold_info.py
# Created by Mickey G for Project Tape Use.
# Upstream project is Timeline

import sys
from datetime import datetime
from plitmongo import Lake

lake = Lake()
datestring, db_type = lake.parse_args(sys.argv[1:])
dbclient = lake.get_db(use_auth=False)
df = lake.get_df("timeline_req_hold_info", "PLIT_TIMELINE_REQ_HOLD_INFO", datestring)
db_names = lake.get_db_names(db_type)

na_table = {"Main_Line_Comment": ""}
df = df.fillna(na_table)

data_time = datetime.now()

for db_name in db_names:
    db = dbclient[db_name]
    # Creates a set of tuples in (B_Unit, REQ_ID), so a dictionary of 
    #   {(B_Unit, REQ_ID): whether this req has been seen} can be created
    # We need the tuple of (B_Unit, REQ_ID) to avoid REQ_ID collision across
    #   multiple business units.
    req_id_tuples = zip(df["Business_Unit"].tolist(), df["Requisition_ID"].tolist())
    seen_req_dict = dict(((b_unit, req_id), False) for b_unit, req_id in req_id_tuples)
    for row in df.itertuples():
        if not seen_req_dict[(row.Business_Unit, row.Requisition_ID)]:
            db.REQ_DATA.update({'$and': [{"Business_Unit": row.Business_Unit}, 
                                         {"REQ_No": row.Requisition_ID}]},
                                {'$set': {
                                    "Holds": []
                                }})
            seen_req_dict[(row.Business_Unit, row.Requisition_ID)] = True

    for row in df.itertuples():
        db.REQ_DATA.update({'$and': [{"Business_Unit": row.Business_Unit}, 
                                     {"REQ_No": row.Requisition_ID}]},
                            {'$addToSet': { "Holds":
                            {"Hold_Line_Number": row.Line_Number,
                             "Hold_Status": row.Hold_Status,
                             "Hold_Type": row.Hold_Type,
                             "Hold_Comment": row.Main_Line_Comment,
                             "Hold_Start_Time": row.Created_DateTime,
                             "Hold_End_Time": row.Last_Update_Timestamp,
                             "Hold_Duration": row.Time_Elapsed,
                             "Hold_Last_comment": row.Last_Comment_Entered}
                            }})
    db.LAST_UPDATED.update({'dbname': "REQ_DATA/Hold"},
                           {'$set': {"last_updated_time": data_time}}, upsert=True)

lake.end()