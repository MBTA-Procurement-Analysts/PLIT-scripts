
import os
import shutil
os.chdir('/home/rubix/Desktop/Project-Ducttape/scripts/')
import deltaGQL as dl

# Check if spark session is already running or create new one                                                                                                                                              
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.appName("DeltaTemplate").getOrCreate()

# Define path for storing IOH csv files
csv_destination_path = '/home/rubix/O_drive_mnt_pt/INVENTORY/Warehouse Logistics/Reports & Publications/Kate Cairns/Inventory On Hand for Scott/DATA/mondaycsvfiles/'

# Remove unnecessary older file
shutil.rmtree(csv_destination_path+'Previous_Monday')

# Rename latest Monday folder and file as previous Monday
os.rename(csv_destination_path+'Latest_Monday', csv_destination_path+'Previous_Monday')
os.rename(csv_destination_path+'Previous_Monday/IOH_Latest_Monday.csv', csv_destination_path+'Previous_Monday/IOH_Previous_Monday.csv')   
print('Previous Monday IOH csv file ready')

# Load lastest Monday file from delta lake and store at defined path as latest Monday
df = dl.load_latest_version(os.getenv("RUBIXTAPEDELTAPATH")+"/GQL_PL_INV_BY_BASE")
print("Latest IOH version is: {}".format(dl.get_latest_version(os.getenv("RUBIXTAPEDELTAPATH")+"/GQL_PL_INV_BY_BASE")))
df.repartition(1).write.csv(path=csv_destination_path+'Latest_Monday/', mode="append", header="true")
all_files = os.listdir(csv_destination_path+'Latest_Monday/')
csv_files = []
for i in all_files:
	 if i.endswith('.csv'):
	    csv_files.append(i)
os.rename(csv_destination_path+'Latest_Monday/'+csv_files[0], csv_destination_path+'Latest_Monday/IOH_Latest_Monday.csv')
print('Latest Monday IOH csv file ready')