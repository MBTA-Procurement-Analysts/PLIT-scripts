# monthlyreport-split_po.py
# Created by Mickey G for Project Tape Use.
# Upstream project is Monthly Report

import sys
from plitmongo import Lake
from datetime import datetime
import progressbar

lake = Lake()
datestring, db_type = lake.parse_args(sys.argv[1:])
dbclient = lake.get_db(use_auth=False)
df = lake.get_df("po_split", "NG_NO_TIMING", datestring)
df = lake.get_df("timing", "NG_NO_SPILT_PO_SIDE", datestring)
db_names = lake.get_db_names(db_type)

