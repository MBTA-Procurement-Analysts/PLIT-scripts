import datetime
import pandas as pd
import os.path
import xlsxwriter
import sys
import shutil
import subprocess
import deltaGQL
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import lit
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number
from delta.tables import *
from pyspark.sql.types import *

'''
Check if spark session is already running or create new one
'''
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.getOrCreate()

def upsert_spark(old_df, latest_df, primary_keys):
    '''
    Used for upserting the data in spark dataframe. Finds if there are any new 
    columns in latest dataframe. If there are any then adds them to older version and
    fills it will null. Checks whether the record is already present in older dataframe
    based on primary keys and then updates or inserts records and keeps only latest record
    on the basis of Last_Modified_Timestamp.

    Args:
    old_df: older version as spark dataframe (from delta lake)
    latest_df: current version which is mostly the ducttape output as spark dataframe
    primary_keys: primary keys of the particular table

    Returns:
    Upserted dataframe
    '''

    new_cols = list(set(latest_df.columns)-set(old_df.columns))
    for cols in new_cols:
        old_df = old_df.withColumn(cols, lit(None))
    latest_df_cols = latest_df.columns
    old_df = old_df.select(latest_df_cols)
    df = old_df.unionAll(latest_df)
    primarykeys = list(primary_keys) 
    partition = Window.partitionBy(primarykeys).orderBy(col('Last_Modified_Timestamp').desc())
    df = df.withColumn("temp_row_number", row_number().over(partition))
    df = df.where(df.temp_row_number == 1).drop("temp_row_number")
    return df.orderBy(primarykeys)

def upsert_pandas(old_df, latest_df, primary_keys):
    '''
    Works similar to upsert_spark but used with pandas dataframe.
    
    Args:                                                                                                                                                                                                      old_df: older version as spark dataframe (from delta lake)                                                                                                                                                 latest_df: current version which is mostly the ducttape output as spark dataframe                                                                                                                          primary_keys: primary keys of the particular table                                                                                                                                                      
    Returns:                                                                                                                                                                                              
    Upserted dataframe 
    '''
    df = pd.concat([old_df, latest_df])
    df = df.sort_values(by=['Last_Modified_Timestamp'], ascending=False)
    df['row_number'] = df.groupby(primary_keys).cumcount()
    df = df[df['row_number'] == 0]
    del df['row_number']
    return df


''' Pandas to Spark Dataframe'''
def equivalent_type(pandas_dtype):
    '''
    Converts pandas data types into spark data types.

    Args:
    pandas_dtype: pandas data type for each column passed in as list

    Returns: 
    Spark equivalent of pandas datatypes for each column of the dataframe
    '''
    
    if pandas_dtype == 'datetime64[ns]': return TimestampType()
    elif pandas_dtype == 'int64': return LongType()
    elif pandas_dtype == 'int32': return IntegerType()
    elif pandas_dtype == 'float64': return FloatType()
    else: return StringType()
    
def define_structure(column_name, pandas_dtype):
    '''
    Constructs fields which are required for defining Spark dataframe schema. Each StructField
    is equivalent to a column in a dataframe.
    
    Args:
    column_name: list of all the column names
    pandas_dtypes: respective pandas data type of the columns
    
    Returns:
    Basically returns the column name and its spark data type in a format which
    can be used for creating spark dataframe schema.
    '''

    return StructField(column_name, equivalent_type(pandas_dtype))

def pandas_to_spark(pandas_df):
    '''
    Coverts pandas dataframe to spark dataframe.
    
    Args:
    pandas_df: Pandas dtaframe

    Returns:
    Spark dataframe.
    '''
    
    column_names = list(pandas_df.columns)
    pandas_dtypes = list(pandas_df.dtypes)
    struct_list = []
    for column_name, pandas_dtype in zip(column_names, pandas_dtypes): 
      struct_list.append(define_structure(column_name, pandas_dtype))
    spark_schema = StructType(struct_list)
    return spark_schema



def rename_columns(df):
    '''
    Removes whitespaces, "?", ":", "$_" and "." from column names.
    Regex is need and coerce b/c future default is FALSE
    
    Args:
    df: pandas dataframe

    Returns:
    Returns pandas dataframe with renamed columns.
    
    #df.columns = [x.replace('?', '') for x in [x.replace(':', '') for x in [x.replace('$_', '') for x in [x.replace('.', '') for x in [x.replace(' ', '_') for x in [x.upper() for x in df.columns]]]]]] 
    '''
    df.columns = [x.upper() for x in df.columns]
    df.columns = df.columns.str.replace('[?,:,$,.]', '', regex=True)
    df.columns = [x.strip() for x in df.columns]
    df.columns = df.columns.str.replace(' ', '_', regex=True)
    return df


def add_timestamp(df):
    '''
    Add a column for timestamp for upsert to work. Upsert function uses this column for
    distinguishing between old and new records. This function basically adds a timestamp of when data was pulled.

    Args:
    df: pandas dataframe
    
    Returns:
    Pandas dataframe with additional column named "Last_Modified_Timestamp"
    '''
    df["Last_Modified_Timestamp"] = pd.Series([datetime.datetime.now()] * len(df))
    return df


def get_latest_version(filepath):
    '''
    Goes through the delta lake and finds the latest version of the dataframe. Each time we store data as
    in delta format we get a new version. We can see the list of all versions and timestamps in DeltaTable.

    Args: 
    filepath: path for the folder. Say we are storing a data frame as delta table in a specific folder then filepath
    references to that folder. This folder will contain all the parquet and log files for the sepcific table.

    Returns:
    Version number of the latest version (delta file).
    '''
    df_history = DeltaTable.forPath(spark, filepath).history().toPandas()
    latest_version = df_history["version"][0]
    return latest_version


def load_latest_version(filepath):
    '''
    Loads latest version of data from delta lake. Uses get_latest_version function to find the lastest version number
    and then loads it as Spark dataframe.

    Args:
    filepath: path for the folder. Say we are storing a data frame as delta table in a specific folder then filepath                                                                                       
    references to that folder. This folder will contain all the parquet and log files for the sepcific table.
    
    Returns:
    Latest data from the delta lake as a Spark dataframe.
    '''
    df = spark.read.format("delta").option("versionAsOf", get_latest_version(filepath)).load(filepath)
    return df

def load_human_version(querybasename, hv=None):
    '''
    Goes through the human_version_dictionary and delta lake and finds the respective version of the dataframe. Each time we store data as
    in delta format we get a new version. We can see the list of all versions and timestamps in DeltaTable.

    Args:
    filepath: path for the folder. Say we are storing a data frame as delta table in a specific folder then filepath 
    references to that folder. This folder will contain all the parquet and log files for the sepcific table.   

    Returns:      
    Version number of the latest version (delta file).
    '''
    filepath = "/home/rubix/Desktop/Project-Ducttape/delta/"
    if hv == None:
        df = spark.read.format("delta").option("versionAsOf", get_latest_version(filepath+querybasename)).load(filepath+querybasename)
    else:
        df = spark.read.format("delta").option("versionAsOf", deltaGQL.version_dict.get(querybasename).get(hv)).load(filepath+querybasename)
    return df

def save_as_delta(latest_df, primary_keys, filepath):
    '''
    Saves dataframe in delta format. While saving first verifies if there exists any previous versions,
    if previous version exists, then loads the latest version, upserts the current data and then saves it.
    If there aren't any previous versions then creates a folder in cddt/scripts/deltaGQL/ and saves data inside it.
    Appends the data to previous version.

    Args:
    latest_df: current data (output from ducttape) as a Spark dataframe
    primary_keys: primary keys of the data
    filepath: path for the folder. Say we are storing a data frame as delta table in a specific folder then filepath                                                                                       
    references to that folder. This folder will contain all the parquet and log files for the sepcific table. 

    Returns:
    None.
    '''
    
    if (os.path.exists(filepath) == True):
        print("Pushing data into Delta Lake")
        print("Earlier versions exist. Appending recent data to earlier data")
        old_df = load_latest_version(filepath)
        df = upsert_spark(old_df, latest_df, primary_keys)
        df.write.mode("overwrite").option("mergeSchema", "true").format("delta").save(filepath)
        latest_version = get_latest_version(filepath)
        print("Data saved as version: {}\nNumber of new records: {}\nTotal number of records: {}\nFilepath: {}".format(latest_version, df.count()-old_df.count(), df.count(), filepath))
    else:
        print("Pushing data into Delta Lake")
        print("No early versions of table exist. Creating new filepath")
        latest_df.write.format("delta").save(filepath)
        latest_version = get_latest_version(filepath)
        print("Data saved as version: {}\nTotal number of records: {}\nFilepath: {}".format(latest_version, latest_df.count(), filepath))
        
# Saves file in Excel format if not exists and if exists then overwrite (Path defined as gql_tableau_data folder)
def save_as_excel(latest_df, primary_keys, filepath, filename):
    '''
    Saves file in Excel format. If file already exists, then overwrites.
    These files will be stored at path cddt/gql_tableau_folder.

    Args:
    latest_df: current data (output from ducttape) as a Spark dataframe 
    primary_keys: primary keys of the data   
    filepath: path for the folder. Say we are storing a data frame as delta table in a specific folder then filepath
    references to that folder. This folder will contain all the parquet and log files for the sepcific table.
    filename: name of the excel file. Normally, setname will be excel filename itself

    Return:
    None.
    '''
    
    if (os.path.exists('/home/rubix/Desktop/Project-Ducttape/gql_tableau_data/'+filename+'.xlsx') == True):
        print("Saving file in Excel format")
        print("Early versions exist. Appending recent data to earlier data.")
        old_df = pd.read_excel('/home/rubix/Desktop/Project-Ducttape/gql_tableau_data/'+filename+'.xlsx', sheet_name='sheet1')
        print("Loading last version.")
        latest_df = latest_df.toPandas()
        df = upsert_pandas(old_df, latest_df, primary_keys)
        print("Appending new records to earlier version.")
        print("Number of new records: ", len(df.index.tolist())-len(old_df.index.tolist()))
       # df = df.toPandas()
        #df = df.drop(columns='Last_Modified_Timestamp')
        writer = pd.ExcelWriter('/home/rubix/Desktop/Project-Ducttape/gql_tableau_data/'+filename+'.xlsx', datetime_format='YYYY-MM-DD')
        df.to_excel(writer, index=False, sheet_name='sheet1')
        writer.save()
        print('Saved ' +filename+ ' in excel format in gql_tableau_data folder')
    else:
        print("Saving file in Excel format")
        print("Early version does not exist.")
        df = latest_df.toPandas()
       # df = df.drop(columns='Last_Modified_Timestamp')
        writer = pd.ExcelWriter('/home/rubix/Desktop/Project-Ducttape/gql_tableau_data/'+filename+'.xlsx', datetime_format='YYYY-MM-DD')
        df.to_excel(writer, index=False, sheet_name='sheet1')
        writer.save()
        print('Saved ' +filename+ ' in excel format in gql_tableau_data folder')
        

def save_as(df, primary_keys, filepath, save_file_as = "daily"):
    '''
    Upserts & saves if previous version exists or else just saves spark dataframe in delta format as requested depending on type of load. 
    When saving "daily" or "Monday"(considered as daily load) load will store table as "querybasename-daily_pulls" in delta folder. 
    When saving "monthly" or "yearly" loads, it will save the table as "querybasename" and then deletes the "querybasename-daily_pulls" table for that specific table, 
    so that we can again start storing data only for coming month. "querybasename-daily_pulls" only store data for ongoing month.

    Args:
    df: spark dataframe that is to be pushed in delta lake
    primary_keys: primary_keys for that table
    filepath: delta folder path along with querybasename
    save_file_as: type of load. Can take "daily", "monthly" and "yearly" as args

    Return:
    None.
    '''
    if ((save_file_as == 'monthly')|(save_file_as == 'yearly')):
        print("Load type: {}".format(save_file_as))
        save_as_delta(df, primary_keys, filepath)
        delete_daily_pulls(filepath)
    else:
        print("Load type: daily")
        save_as_delta(df, primary_keys, filepath+"-daily_pulls")

        
def delete_daily_pulls(filepath):
    '''
    Deletes daily_pulls table for a particular query.

    Args:
    filepath: delta folder path along with querybasename 
    
    Return:
    None.
    '''
    if (os.path.exists(filepath+"-daily_pulls") == True):
        shutil.rmtree(filepath+"-daily_pulls")
        print("Deleted filepath {}".format(filepath+"-daily_pulls"))
    else:
        print("Filepath {} does not exists".format(filepath+"-daily_pulls"))
        exit

        
def check_load_type_argument():
    '''                                                                                                                                                                                                    
    Checks for the load type system argument.                                                                                                                                                            
    If argument is passed and has value "monthly" or "yearly", then data is stored in monthly tables (as new version).                                                                                         For rest all of the cases, data is stored in temporary daily pulls tables.                                                                                                                                 
    Return:
    None.
    '''

    if (len(sys.argv)==4):
        exit
    elif ((len(sys.argv)==5)&((sys.argv[4]=="monthly")|(sys.argv[4]=="yearly"))):
        return sys.argv[4]
        

def get_values(dictionary, key):
    '''
    Looks for the key in given dictionary and returns its value.

    Args:
    dictionary: any dictionary 
    key: key whose value we want to return

    Returns:
    Value of the key.
    '''
    return dictionary.get(key)


def historical_pulls(querybasename, quantity, unit):
    '''                                                                                                                                                                                              
    Pulls all the historical data starting from 01/01/2005 till today, in monthly intervals. Runs the tape file of specified parameterised querybasename,                                                  
    pulls data based in specified window and then upserts & saves as monthly delta table. After saving the data, deletes the daily_pulls table for that query.                                           
                                                                                                                                                                                                           
    Args:                                                                                                                                                     
    querybasename: querybasename of the table which we want to pull                                                                                                                                       
    window: time window in terms of months. Only takes whole numbers.                                                                                                                                      
                                                                                                                                                                                                          
    Returns:                                                                                                                                                                                               
    None.                                                                                                                                                                                                  
    '''
    file="/home/rubix/Desktop/Project-Ducttape/tape-"+querybasename.lower()+".sh"
    start_date = datetime.date(2005, 1, 1)
    end_date = start_date
    curr_date = datetime.date.today()                                                                                                                                                                     
 #  curr_date = datetime.date(2021, 7, 31)
    while end_date < curr_date:
        if unit=='months':
            end_date = start_date + relativedelta(months=int(quantity)) - datetime.timedelta(days=1)
        elif unit=='years':
            end_date = start_date + relativedelta(years=int(quantity)) - datetime.timedelta(days=1)
        if end_date <= curr_date:
            output = subprocess.call([file, "both", "local", start_date.strftime("%m/%d/%Y"), end_date.strftime("%m/%d/%Y"), "monthly"])
            start_date = end_date + datetime.timedelta(days=1)
        else:
            end_date = curr_date
            output = subprocess.call([file, "both", "local", start_date.strftime("%m/%d/%Y"), end_date.strftime("%m/%d/%Y"), "monthly"])
            start_date = end_date + datetime.timedelta(days=1)
    print("Latest delta version of {} is version {}".format(querybasename, get_latest_version(os.getenv("RUBIXTAPEDELTAPATH")+'/'+querybasename)))

    
def pull_historical_data(querybasename, quantity, unit=None):
    '''
    Function which checks whether its monthly or yearly window and uses proper function to run bash scripts within required window.

    Args:
    querybasename: querybasename of the table which we want to pull 
    quantity: time window. Only takes whole numbers.
    unit: unit of the window. Unit should be either "monthly" or "yearly".

    Returns:
    None.
    '''
    if unit not in ('years', 'yearly','monthly','months', None):
        print("Please check value of Unit. Unit should be either monthly or yearly.")
    elif (unit=='monthly')|(unit=='months'):
        historical_pulls(querybasename, quantity, 'months')
    else:
        historical_pulls(querybasename, quantity, 'years')

def check_argument(arg_num):
    '''
    Checks whether valid and required number of arguments are provided. Only if both the conditions are satisfied then returns the arguments.
    Prevents breaking delta lake by running invalid time window.

    Args:
    arg_num: number of arguments expected.

    Returns:
    Value of the argument at index arg_num.
    '''
    if (len(sys.argv)==arg_num):
        arg_val = None
        exit
    elif ((len(sys.argv)==(arg_num+1))):
        arg_value = sys.argv[arg_num]
        return arg_value

#function to create subset for qty
def qty_subset(raw_df):
    qty_subset = raw_df[['Unit', 'Item', 'Qty On Hand', 'Pull_Date']]
    return qty_subset

#function to create subset for value
def value_subset(raw_df):
    value_subset = raw_df[['Item', 'On Hand Value', 'Pull_Date']]
    return value_subset

#function to pivot subset for qty
def pivot_qty_subset(qty_subset):
    qty_subset_pivoted = pd.pivot_table(qty_subset, values='QTY_ON_HAND', index=['PULL_DATE', 'UNIT'], columns=['ITEM'])
    return qty_subset_pivoted

#function to pivot subset for value
def pivot_value_subset(value_subset):
    value_subset_pivoted = pd.pivot_table(value_subset, values='ON_HAND_VALUE', index=['PULL_DATE'], columns=['ITEM'])
    return value_subset_pivoted

#function to extract date from filename
def extract_pull_date(filename):
    date_temp = filename.lstrip('KJ_INV_BY_BASE_').rstrip('.xlsx')
    pull_date = datetime.datetime.strptime(date_temp, '%d%b%Y').date()
    return pull_date
