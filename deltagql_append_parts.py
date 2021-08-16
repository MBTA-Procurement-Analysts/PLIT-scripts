import os
import sys
import xlsxwriter
import pandas as pd

# Get required variables which were passed as arguments:
setname = sys.argv[1]
querybasename = sys.argv[2]
date = sys.argv[3]

# Declare path for selenium files:
datapath = os.getenv("RUBIXTAPEDATAPATH")
ducttapefolder = datapath+"/"+setname+"/"+date

# Read all parts and append them in one dataframe:
print("Combining both parts")
files = os.listdir(ducttapefolder)  
df = pd.DataFrame()
for file in files:
    if file.endswith('.xlsx'):
        df = df.append(pd.read_excel(ducttapefolder+"/"+file, skiprows=1))

# Add a dummy row on top:
df.loc[-1] = df.columns.tolist()
df.index = df.index + 1
df = df.sort_index()

# Write appended data as new excel file:
writepath = ducttapefolder+"/"+querybasename+"-"+date+".xlsx"
writer = pd.ExcelWriter(writepath, engine='xlsxwriter', datetime_format='YYYY-MM-DD')
df.to_excel(writer, index=False, sheet_name='sheet1')
writer.save()
print("Number of records after combining both parts: {}".format(len(df.index)))
