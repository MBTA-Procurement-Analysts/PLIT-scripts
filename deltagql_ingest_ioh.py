import os
import deltaGQL as dl
import datetime
import numpy as np
from datetime import date
from datetime import datetime as dt
import pandas as pd
from numpy import nan as NA
import glob

# Check if spark session is already running or create new one                                                                                                                                              
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.appName("DeltaTemplate").getOrCreate()

# Go to Inventory On Hand for Scott folder and get a list of all files
path = '/home/rubix/O_drive_mnt_pt/INVENTORY/Warehouse Logistics/Reports & Publications/Kate Cairns/Inventory On Hand for Scott/DATA/'
files = os.listdir(path)
print("Total number of files: {}".format(len(files)))

# Filter only excel files
files1 = []
for i in files:
    if i.endswith('.xlsx'):
        files1.append(i)

# Sort filenames list based on dates in file name        
files2 = [i.strip('.xlsx') for i in [i.strip('KJ_INV_BY_BASE_') for i in files1]]
files2.sort(key=lambda date: dt.strptime(date, "%d%b%Y"))

files3=[]
for i in files2:
    j = "KJ_INV_BY_BASE_"+i+".xlsx"
    files3.append(j)
    
print("Total number of files after arranging chronologically: {}".format(len(files3)))

# Declare schema for IOH
schema ={'Unit': object, 'Item': object, 'Qty On Hand': float, 'On Hand Value': float, 'Last Ann': float, 'Util Type': object,'$ LTM Demand': float, 'Descr': object, 'Descr.1': object, 'Descr.2': object, 'Status Current': object,'Reorder Pt': int, 'Reord Qty': int, 'Max Qty': float, 'Pull_Date': datetime, 'Avg Value': float,'Std UOM': object, 'Average Unit Cost': float, 'Check': object, 'RL Deactivate Item?': object,'Replenishment Flag': object, 'Unnamed: 4': object, 'Avg Cost': float, 'RL List': object, 'Min': object}

# Read excel files chronologically, rename columns, add timestamp, convert to spark dataframe and ingest into delta table 
i=1
for file in files3:
    if file.endswith('.xlsx'):
        df = pd.DataFrame()
        df = pd.read_excel(path+file, skiprows=1, dtype=schema)
        print("File {} read successfully".format(file))
        df.columns= [x.replace('?', '') for x in [x.replace(':', '') for x in [x.replace('$_', '') for x in [x.replace('.', '') for x in [x.replace(' ', '_') for x in [x.upper() for x in df.columns]]]]]]
        df["Last_Modified_Timestamp"] = pd.Series([datetime.datetime.combine(datetime.datetime.strptime(str(dl.extract_pull_date(file)), '%Y-%m-%d').date(), datetime.datetime.now().time())]*len(df))
        df.rename({'UNIT': 'IOH-UNIT', 'ITEM': 'IOH-ITEM_ID', 'QTY_ON_HAND': 'IOH-QTY_ON_HAND', 'ON_HAND_VALUE': 'IOH-ON_HAND_VALUE', 'LAST_ANN': 'IOH-LAST_ANNUAL_DEMAND', 'UTIL_TYPE': 'IOH-UTILIZATION_TYPE', 'LTM_DEMAND': 'IOH-LTM_DEMAND', 'DESCR': 'IOH-ITEM_DESCR', 'DESCR1': 'IOH-FMIS_ITEM_GROUP', 'DESCR2': 'IOH-FMIS_ITEM_FAMILY', 'STATUS_CURRENT': 'IOH-CURRENT_STATUS', 'REORDER_PT': 'IOH-REORDER_POINT', 'REORD_QTY': 'IOH-REORDER_QTY', 'MAX_QTY': 'IOH-MAX_QTY'}, axis=1, inplace=True)
        print("Data transformed")
        spark_schema = dl.pandas_to_spark(df)
        spark_df = spark.createDataFrame(df, spark_schema)
        print("Spark Dataframe created")
        spark_df.write.mode("append").option("mergeSchema", "true").format("delta").save(path+"DELTA/GQL_IOH4")
        print("File {} ingested as version {} successfully".format(file, i))
        i=i+1
