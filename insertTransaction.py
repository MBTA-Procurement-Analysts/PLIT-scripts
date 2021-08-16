import sys
import time
import datetime as datetime
import pandas as pd
from plitmongo import Lake

lake = Lake()
datestring, db_type = lake.parse_args(sys.argv[1:])
db_client = lake.get_db(use_auth = False)
df = lake.get_df("inv_transaction", "INV-TRANSACTION", datestring, converters = {'Transaction Type': str, 'Item': str})
db_names = lake.get_db_names(db_type)

#Transform

#1 fill NAs
na_table = {"Dest_Unit": "",
            "Order_No": "",
            "Area": "",
            "Lev_1": "",
            "Lev_2": "",
            "Lev_3": "",
            "Lev_4": "",
            "Type": "",
            "Location": "",
            "Ship_Cust": ""
}
df = df.fillna(value = na_table)

            
#2 Creating Index
#2.1 Timestamp convertion

df.loc[:, 'Timestamp'] = [i.timestamp() for i in df['Transaction_Time_Stamp']]

#2.2 Concatenate Strings to create unique indexes
# Unit_Item_Timestamp_SeqNum = PKs
PKs = df['Unit'].map(str) + '_' + df['Item'].map(str) + '_' + df['Timestamp'].map(int).map(str) + '_' + df['Seq_Number'].map(str)   #Concat Primary Keys
# PKs_Area,Lev1,2,3,4 = Index
df.loc[:,'TransIndex'] = PKs + '_' + df['Area'].map(str) + df['Lev_1'].map(str) + df['Lev_2'].map(str) + df['Lev_3'].map(str) + df['Lev_4'].map(str)

#2.3

duplist = df.duplicated(['TransIndex'], keep = False)
df.loc[duplist,'rank'] = df[duplist].groupby(by = "TransIndex")['Qty'].rank(method = 'first').astype(int)
df = df.fillna(value = {'rank': 0})
df.loc[:,'TransIndex'] = df['TransIndex'] + "_" + df['rank'].map(int).map(str)


Indexedlen = len(df['TransIndex'].unique())
rawlen = df.shape[0]
if Indexedlen == rawlen:
    print("Indexes are unique.")
else:
    print("Indexed length: ", Indexedlen)
    print("Raw table length: ", rawlen)
    print("Indexes are not unique! Please check.")


#Load
for db_name in db_names:
    uniqueTrxnIDs = dict((transindex, False)
                         for transindex in df['TransIndex'].unique().tolist())
    db = db_client[db_name]

    for row in df.itertuples():
        if not uniqueTrxnIDs[row.TransIndex]:         
            db.INV_TRANSACTIONS.update_one({'Trans_Index': row.TransIndex},{
                '$set': {
                    'Trans_Index': row.TransIndex,
                    'Seq_Number': row.Seq_Number,
                    'Transaction_Type': row.Transaction_Type,
                    'Unit': row.Unit,
                    'Dest_Unit': row.Dest_Unit,
                    'Transaction_Date': row.Transaction_Date,
                    'Transaction_Time_Stamp': row.Transaction_Time_Stamp,
                    'Order_No': row.Order_No,
                    'Item': row.Item,
                    'Qty': row.Qty,
                    'Avg_Matl_Cost': row.Avg_Matl_Cost,
                    'Total_Cost': row.Total_Cost,
                    'User': row.User,
                    'Area_Info':{
                        'Area': row.Area,
                        'Lev_1': row.Lev_1,
                        'Lev_2': row.Lev_2,
                        'Lev_3': row.Lev_3,
                        'Lev_4': row.Lev_4
                        },
                    'Type': row.Type,
                    'In_Demand':{
                        'Location': row.Location,
                        'Ship_Cust': row.Ship_Cust
                        },
                    'Item_Group': row.Item_Group,
                    'Util_Type': row.Util_Type,
                    'UOM':row.UOM,
                    'FIFO_Included': False,
                    'FIFO_Partial': 0
        
                }
            }, upsert = True)
            uniqueTrxnIDs[row.TransIndex] = True

    db.LAST_UPDATED.update({'dbname':"INV_TRANSACTIONS"} ,{
            '$set': {'last_updated_time': datetime.datetime.now()}}, upsert = True)
        
                    
lake.end()
    
                         
    
