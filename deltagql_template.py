# deltagql_template.py
# Created by Delta_GQL_TEAM
# deltagql_template_test

import datetime
import pandas as pd
import deltaGQL
import sys
import os
from pyspark.sql.functions import *

# Check if spark session is already running or create new one
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.appName("DeltaTemplate") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


# Get setname, querybasename and date passed in as arguments from tape file
setname = sys.argv[1]
querybasename = sys.argv[2]
date = sys.argv[3]

# Check the file output format (delta format if specified, otherwise always saves as excel)
save_file_as = None
save_file_as = deltaGQL.check_load_type_argument()

# Specify path for reading Ducttape file, saving Delta file, gql_tableau excel filename and primary keys of the table
datapath = os.getenv("RUBIXTAPEDATAPATH") 
deltapath = os.getenv("RUBIXTAPEDELTAPATH") 
ducttapefile = datapath+"/"+setname+"/"+date+"/"+querybasename+"-"+date+".xlsx"
deltafilepath = deltapath+"/"+querybasename

# Get schema of the table
get_schema = deltaGQL.schema
schema = deltaGQL.get_values(get_schema, querybasename)

# Read latest data(fed by ducttape) by skipping first row
latest_df = pd.read_excel(ducttapefile, sheet_name='sheet1', skiprows=1, dtype=schema)
print("Ducttape file read successfully")

# Remove whitespaces and . from column names
latest_df = deltaGQL.rename_columns(latest_df)

# Add column for timestamp for upsert to work
latest_df = deltaGQL.add_timestamp(latest_df)

# Convert Pandas data types into Spark equivalent and create Spark Dataframe
spark_schema = deltaGQL.pandas_to_spark(latest_df)
latest_df = spark.createDataFrame(latest_df, spark_schema)
print("Ducttape file successfully converted into Spark Dataframe")

# Get the primary keys of the table
table_pkeydictionary = deltaGQL.primary_keys
primary_keys = deltaGQL.get_values(table_pkeydictionary, querybasename)

# Save file as excel or delta (as specified in argument)
deltaGQL.save_as(latest_df, primary_keys, deltafilepath, save_file_as)
