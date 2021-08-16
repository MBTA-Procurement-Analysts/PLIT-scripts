import datetime
import pandas as pd
import sys
import os
import shutil
import deltaGQL

# Get querybasename
querybasename = sys.argv[1]

# Get filepath of original delta folder
deltapath = os.getenv("RUBIXTAPEDELTAPATH")
deltafilepath = deltapath+"/"+querybasename

# Load latest file
df = deltaGQL.load_latest_version(deltafilepath)

# Store data in temporary folder
newfilepath = deltafilepath+"-single_version"
df.write.format("delta").save(newfilepath)

# Delete original folder along with its contents
#shutil.rmtree(deltafilepath)
tempfilepath = deltafilepath+"-separatepulls"
os.rename(deltafilepath, tempfilepath)

# Rename temporary folder to original one
os.rename(newfilepath, deltafilepath)

# Data integrity checks
latest_version = deltaGQL.get_latest_version(deltafilepath)
df = deltaGQL.load_latest_version(deltafilepath)
print("Data saved as version: {}\nDelta file name: {}\nNumber of Records: {}".format(latest_version, deltafilepath, df.count()))
