import pandas as pd
import pyspark
from datetime import datetime
from pyspark.sql.types import *
import graphene
import flask
import flask_graphql
import deltaGQL
import os
import gc
from flask import Flask
from flask_graphql import GraphQLView

# Check if spark session is already running or create new one                                                                                                                                               
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.getOrCreate()

# Load latest data:                                                                                                                                                                                         
deltapath = os.getenv("RUBIXTAPEDELTAPATH")

# Load monthly dfs:                                                                                                                                             
monthly_GQL_PO_AW = deltaGQL.load_latest_version(deltapath+"/GQL_PO_AW")
monthly_GQL_PO_APPR_HDR_VW = deltaGQL.load_latest_version(deltapath+"/GQL_PO_APPR_HDR_VW")
monthly_GQL_EOAW_STEPINST = deltaGQL.load_latest_version(deltapath+"/GQL_EOAW_STEPINST")
monthly_GQL_EOAW_USERINST = deltaGQL.load_latest_version(deltapath+"/GQL_EOAW_USERINST")
monthly_GQL_PSOPRDEFN = deltaGQL.load_latest_version(deltapath+"/GQL_PSOPRDEFN")
print("Loaded monthly tables")

# Load daily dfs:
daily_GQL_PO_AW = deltaGQL.load_latest_version(deltapath+"/GQL_PO_AW-daily_pulls")
daily_GQL_PO_APPR_HDR_VW = deltaGQL.load_latest_version(deltapath+"/GQL_PO_APPR_HDR_VW-daily_pulls")
daily_GQL_EOAW_STEPINST = deltaGQL.load_latest_version(deltapath+"/GQL_EOAW_STEPINST-daily_pulls")
daily_GQL_EOAW_USERINST = deltaGQL.load_latest_version(deltapath+"/GQL_EOAW_USERINST-daily_pulls")
daily_GQL_PSOPRDEFN = deltaGQL.load_latest_version(deltapath+"/GQL_PSOPRDEFN-daily_pulls")
print("Loaded daily tables")

# Load pks:
table_pkeydictionary = deltaGQL.primary_keys
pk_GQL_PO_AW = deltaGQL.find_keys(table_pkeydictionary, "GQL_PO_AW")
pk_GQL_PO_APPR_HDR_VW = deltaGQL.find_keys(table_pkeydictionary, "GQL_PO_APPR_HDR_VW")
pk_GQL_EOAW_STEPINST = deltaGQL.find_keys(table_pkeydictionary, "GQL_EOAW_STEPINST")
pk_GQL_EOAW_USERINST = deltaGQL.find_keys(table_pkeydictionary, "GQL_EOAW_USERINST")
pk_GQL_PSOPRDEFN = deltaGQL.find_keys(table_pkeydictionary, "GQL_PSOPRDEFN")
print("Found primary keys")

# Upsert data to get latest data:
GQL_PO_AW = deltaGQL.upsert_spark(monthly_GQL_PO_AW , daily_GQL_PO_AW , pk_GQL_PO_AW)
GQL_PO_APPR_HDR_VW = deltaGQL.upsert_spark(monthly_GQL_PO_APPR_HDR_VW , daily_GQL_PO_APPR_HDR_VW , pk_GQL_PO_APPR_HDR_VW)
GQL_EOAW_STEPINST = deltaGQL.upsert_spark(monthly_GQL_EOAW_STEPINST , daily_GQL_EOAW_STEPINST , pk_GQL_EOAW_STEPINST)
GQL_EOAW_USERINST = deltaGQL.upsert_spark(monthly_GQL_EOAW_USERINST , daily_GQL_EOAW_USERINST , pk_GQL_EOAW_USERINST)
GQL_PSOPRDEFN = deltaGQL.upsert_spark(monthly_GQL_PSOPRDEFN , daily_GQL_PSOPRDEFN , pk_GQL_PSOPRDEFN)
print("Merged monthly and daily tables")

del [[monthly_GQL_PO_AW, monthly_GQL_PO_APPR_HDR_VW, monthly_GQL_EOAW_STEPINST, monthly_GQL_EOAW_USERINST, monthly_GQL_PSOPRDEFN, daily_GQL_PO_AW, daily_GQL_PO_APPR_HDR_VW, daily_GQL_EOAW_STEPINST, daily_GQL_EOAW_USERINST, daily_GQL_PSOPRDEFN]]
gc.collect()
print("Running Query")

alldfs = [var for var in dir() if isinstance(eval(var), pyspark.sql.dataframe.DataFrame)]

num = []
name = []
for df in alldfs:
    num.append(eval(df).count())
    name.append(df)
print(zip(name, num))


# Create views:                                                                                                                                                                                             
views = []
for df in alldfs:
    eval(df).createOrReplaceTempView(df)
    views.append(df)
print(views)

# Run SQL:
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
sqlDF = spark.sql('''SELECT DISTINCT A.BUSINESS_UNIT, A.PO_ID, date_format(B.PO_DT, 'MM/dd/yyyy') AS PO_DT, B.PO_STATUS, D.OPRID, K.OPRDEFNDESC, from_unixtime(unix_timestamp(D.DTTM_CREATED, "yyyy-MM-dd HH:mm:ss"),'MM/dd/yyyy hh:mm:ss aa') AS FORMATTED_DTTM_CREATED, from_unixtime(unix_timestamp(D.EOAWDTTM_MODIFIED, "yyyy-MM-dd HH:mm:ss"),'MM/dd/yyyy hh:mm:ss aa') AS FORMATTED_EOAWDTTM_MODIFIED, (D.EOAWDTTM_MODIFIED - D.DTTM_CREATED) AS PENDING_TIME, D.EOAWORIG_OPRID, D.EOAWSTEP_STATUS, D.EOAWSTEP_INSTANCE
  FROM GQL_PO_AW A, GQL_PO_APPR_HDR_VW B, GQL_EOAW_STEPINST C, GQL_EOAW_USERINST D, GQL_PSOPRDEFN K
  WHERE ( B.BUSINESS_UNIT = A.BUSINESS_UNIT
     AND B.PO_ID = A.PO_ID
     AND A.EOAWTHREAD_ID = C.EOAWTHREAD_ID
     AND A.EOAWPRCS_ID = C.EOAWPRCS_ID
     AND C.EOAWDEFN_ID = A.EOAWDEFN_ID
     AND C.EOAWSTEP_INSTANCE = D.EOAWSTEP_INSTANCE
     AND A.EOAWTHREAD_ID = (SELECT MIN( E.EOAWTHREAD_ID)
  FROM GQL_PO_AW E
  WHERE E.BUSINESS_UNIT = A.BUSINESS_UNIT
     AND E.PO_ID = A.PO_ID
     AND E.LINE_NBR = A.LINE_NBR
     AND E.EOAWTHREAD_STATUS <> 'T')
     AND C.EOAWTHREAD_ID = (SELECT MIN(  F.EOAWTHREAD_ID)
  FROM GQL_PO_AW F, GQL_PO_APPR_HDR_VW G, GQL_EOAW_STEPINST H, GQL_EOAW_USERINST I
  WHERE G.BUSINESS_UNIT = A.BUSINESS_UNIT
     AND G.PO_ID = A.PO_ID
     AND G.BUSINESS_UNIT = F.BUSINESS_UNIT
     AND G.PO_ID = F.PO_ID
     AND G.HOLD_STATUS = 'N'
     AND F.EOAWTHREAD_ID = H.EOAWTHREAD_ID
     AND F.EOAWPRCS_ID = H.EOAWPRCS_ID
     AND H.EOAWDEFN_ID = F.EOAWDEFN_ID
     AND H.EOAWSTEP_INSTANCE = I.EOAWSTEP_INSTANCE
     AND F.EOAWTHREAD_ID = (SELECT MIN( J.EOAWTHREAD_ID)
  FROM GQL_PO_AW J
  WHERE J.BUSINESS_UNIT = F.BUSINESS_UNIT
     AND J.PO_ID = F.PO_ID
     AND J.LINE_NBR = F.LINE_NBR
     AND J.EOAWTHREAD_STATUS <> 'T'))
     AND K.OPRID = D.OPRID
     AND A.PO_ID IN ('9000009345','9000008849','9000008896')
     AND A.PO_ID = '9000008896')
  ORDER BY 2, 8, 12''')

sqlDF.persist()
print("Data loaded in cache")

print("Total records: {}".format(sqlDF.count()))

sqlDF.select("BUSINESS_UNIT", "PO_ID", "PO_DT", "PO_STATUS", "OPRID", "OPRDEFNDESC", "FORMATTED_DTTM_CREATED", "FORMATTED_EOAWDTTM_MODIFIED", "PENDING_TIME", "EOAWORIG_OPRID", "EOAWSTEP_STATUS", "EOAWSTEP_INSTANCE").show()