import os
import pandas as pd
import pyspark
from datetime import datetime
from pyspark.sql.types import *
import graphene
import flask
import flask_graphql
import gc
from flask import Flask
from flask_graphql import GraphQLView
#os.chdir("/home/rubix/Desktop/Project-Ducttape/scripts/")
import deltaGQL

# Check if spark session is already running or create new one
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.appName("DeltaTemplate").getOrCreate()

# Load latest data:
deltapath = os.getenv("RUBIXTAPEDELTAPATH")

# Load prod dfs:
production_gql_req_hdr = deltaGQL.load_latest_version(deltapath+"/GQL_REQ_HDR")
production_gql_req_dflt_tbl = deltaGQL.load_latest_version(deltapath+"/GQL_REQ_DFLT_TBL")
production_gql_req_line = deltaGQL.load_latest_version(deltapath+"/GQL_REQ_LINE")
production_gql_mb_req_hold_hdr = deltaGQL.load_latest_version(deltapath+"/GQL_MB_REQ_HOLD_HDR")
production_gql_req_ln_distrib = deltaGQL.load_latest_version(deltapath+"/GQL_REQ_LN_DISTRIB")
production_gql_pv_req_total = deltaGQL.load_latest_version(deltapath+"/GQL_PV_REQ_TOTAL")
print("Loaded monthly tables")

# Load dev dfs: 
dev_gql_req_hdr = deltaGQL.load_latest_version(deltapath+"/GQL_REQ_HDR-daily_pulls")
dev_gql_req_dflt_tbl = deltaGQL.load_latest_version(deltapath+"/GQL_REQ_DFLT_TBL-daily_pulls")
dev_gql_req_line = deltaGQL.load_latest_version(deltapath+"/GQL_REQ_LINE-daily_pulls")
dev_gql_mb_req_hold_hdr = deltaGQL.load_latest_version(deltapath+"/GQL_MB_REQ_HOLD_HDR-daily_pulls")
dev_gql_req_ln_distrib = deltaGQL.load_latest_version(deltapath+"/GQL_REQ_LN_DISTRIB-daily_pulls")
dev_gql_pv_req_total = deltaGQL.load_latest_version(deltapath+"/GQL_PV_REQ_TOTAL-daily_pulls")
print("loaded daily tables")

# Load pks: 
table_pkeydictionary = deltaGQL.primary_keys
primary_keys_gql_req_hdr = deltaGQL.find_keys(table_pkeydictionary, "GQL_REQ_HDR")
primary_keys_gql_req_dflt_tbl = deltaGQL.find_keys(table_pkeydictionary, "GQL_REQ_DFLT_TBL")
primary_keys_gql_req_line = deltaGQL.find_keys(table_pkeydictionary, "GQL_REQ_LINE")
primary_keys_gql_mb_req_hold_hdr = deltaGQL.find_keys(table_pkeydictionary, "GQL_MB_REQ_HOLD_HDR")
primary_keys_gql_req_ln_distrib = deltaGQL.find_keys(table_pkeydictionary, "GQL_REQ_LN_DISTRIB")
primary_keys_gql_pv_req_total = deltaGQL.find_keys(table_pkeydictionary, "GQL_PV_REQ_TOTAL")
print("Found primary keys")

# Upsert data to get latest data:
GQL_REQ_HDR = deltaGQL.upsert_spark(production_gql_req_hdr, dev_gql_req_hdr, primary_keys_gql_req_hdr)
GQL_REQ_DFLT_TBL = deltaGQL.upsert_spark(production_gql_req_dflt_tbl , dev_gql_req_dflt_tbl ,primary_keys_gql_req_dflt_tbl)
GQL_REQ_LINE = deltaGQL.upsert_spark(production_gql_req_line , dev_gql_req_line ,primary_keys_gql_req_line)
GQL_MB_REQ_HOLD_HDR = deltaGQL.upsert_spark(production_gql_mb_req_hold_hdr , dev_gql_mb_req_hold_hdr ,primary_keys_gql_mb_req_hold_hdr)
GQL_REQ_LN_DISTRIB = deltaGQL.upsert_spark(production_gql_req_ln_distrib , dev_gql_req_ln_distrib ,primary_keys_gql_req_ln_distrib)
GQL_PV_REQ_TOTAL = deltaGQL.upsert_spark(production_gql_pv_req_total , dev_gql_pv_req_total ,primary_keys_gql_pv_req_total)
print("Merged monthly and daily tables")

del [[production_gql_req_hdr, production_gql_req_dflt_tbl, production_gql_req_line, production_gql_mb_req_hold_hdr, production_gql_req_ln_distrib, production_gql_pv_req_total, dev_gql_req_hdr, dev_gql_req_dflt_tbl, dev_gql_req_line, dev_gql_mb_req_hold_hdr, dev_gql_req_ln_distrib, dev_gql_pv_req_total]]
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
sqlDF = spark.sql('''SELECT DISTINCT A.BUSINESS_UNIT, A.REQ_ID, A.HOLD_STATUS, A.REQ_DT, A.ORIGIN, A.REQUESTOR_ID, A.APPROVAL_DT, CASE WHEN  B.BUYER_ID = '' THEN 'UNASSIGNED' ELSE  B.BUYER_ID END AS BUYER, A.LAST_DTTM_UPDATE, CASE WHEN  B.BUYER_ID = '' THEN 'Unassigned' ELSE 'Assigned' END AS BUYER_ASSIGN, CASE WHEN  E.MB_HOLD_STATUS IN ('REQ') THEN 'Hold Process Requested' WHEN  E.MB_HOLD_STATUS IN ('ACC') THEN 'Hold Process Completed' ELSE 'Hold NOT Requested' END AS HOLD_REQ_PROCESS, CASE WHEN  A.USER_HDR_CHAR1 = 'Y' THEN 'Requested' ELSE 'Not Requested' END OUT_TO_BID, G.REQ_TOTAL, C.LINE_NBR
  FROM GQL_REQ_HDR A, GQL_REQ_DFLT_TBL B, GQL_REQ_LINE C, (GQL_REQ_HDR D LEFT OUTER JOIN  GQL_MB_REQ_HOLD_HDR E ON  D.BUSINESS_UNIT = E.BUSINESS_UNIT AND D.REQ_ID = E.REQ_ID ), GQL_REQ_LN_DISTRIB F, GQL_PV_REQ_TOTAL G
  WHERE ( A.BUSINESS_UNIT = B.BUSINESS_UNIT
     AND A.REQ_ID = B.REQ_ID
     AND A.BUSINESS_UNIT = C.BUSINESS_UNIT
     AND A.REQ_ID = C.REQ_ID
     AND C.SOURCE_STATUS <> 'C'
     AND A.REQ_STATUS = 'A'
     AND A.BUSINESS_UNIT = D.BUSINESS_UNIT
     AND A.REQ_ID = D.REQ_ID
     AND C.CURR_STATUS = 'A'
     AND C.BUSINESS_UNIT = F.BUSINESS_UNIT
     AND C.REQ_ID = F.REQ_ID
     AND C.LINE_NBR = F.LINE_NBR
     AND B.BUYER_ID <> ''
     AND A.ORIGIN <> 'TSK'
     AND A.BUSINESS_UNIT = G.BUSINESS_UNIT
     AND A.REQ_ID = G.REQ_ID
     AND C.INVENTORY_SRC_FLG = 'N'
     AND F.KK_CLOSE_FLAG <> 'Y')
  ORDER BY 4, 1, 2''')

sqlDF.persist()
#sqlDF.cache()
print("Data loaded in cache")

print("Total records: {}".format(sqlDF.count()))

'''
Req_ID = 'EV210245MO'
Ln_No = 1
a = sqlDF.filter((sqlDF["REQ_ID"] == Req_ID) & (sqlDF["LINE_NBR"] == Ln_No)).select("BUSINESS_UNIT").rdd.flatMap(lambda x: x).take(12)
print(a)
'''

sqlDF.select("BUSINESS_UNIT", "REQ_ID", "LINE_NBR", "BUYER", "BUYER_ASSIGN", "REQUESTOR_ID", "ORIGIN", "REQ_TOTAL").show(3)

# Schema for GraphQL API:
class Schema_PO_LINE(graphene.ObjectType):
    Requisition_ID = graphene.String(required=True)
    Line_Number = graphene.String(required=True)
    Business_Unit = graphene.String()
    Hold_From_Further_Processing = graphene.String()
    Requisition_Date = graphene.String()
    Origin = graphene.String()
    Requester = graphene.String()
    Date_of_Approval = graphene.String()
    Buyer = graphene.String()
    Last_Change_Date = graphene.String()
    Buyer_Assignment = graphene.String()
    Hold_Req_Process = graphene.String()
    Out_to_Bid = graphene.String()
    Req_Total = graphene.String()

class Schema_PO_HDR(graphene.ObjectType):
    Requisition_ID = graphene.String(required=True)
    Line_Number = graphene.String()
    Business_Unit = graphene.String()
    Hold_From_Further_Processing = graphene.String()
    Requisition_Date = graphene.String()
    Origin = graphene.String()
    Requester = graphene.String()
    Date_of_Approval = graphene.String()
    Buyer = graphene.String()
    Last_Change_Date = graphene.String()
    Buyer_Assignment = graphene.String()
    Hold_Req_Process = graphene.String()
    Out_to_Bid = graphene.String()
    Req_Total = graphene.String()

# Function for GraphQL:
class Query(graphene.ObjectType):
    PO_HDR = graphene.Field(Schema_PO_HDR, Requisition_ID=graphene.String())
    PO_LINE = graphene.Field(Schema_PO_LINE, Requisition_ID=graphene.String(), Line_Number = graphene.String())

    def resolve_PO_HDR(self, info, Requisition_ID):
        return Schema_PO_HDR(Business_Unit = sqlDF.filter(sqlDF["REQ_ID"] == Requisition_ID).select("BUSINESS_UNIT").rdd.map(lambda row : row[0].encode("ascii", "ignore")).collect(),
                          Requisition_ID = Requisition_ID,
                          Line_Number = sqlDF.filter(sqlDF["REQ_ID"] == Requisition_ID).select("LINE_NBR").rdd.map(lambda row : row[0]).collect(),
                          Requisition_Date = sqlDF.filter(sqlDF["REQ_ID"] == Requisition_ID).select("REQ_DT").rdd.map(lambda row : row[0].strftime('%m/%d/%Y')).collect(),
                          Date_of_Approval = sqlDF.filter(sqlDF["REQ_ID"] == Requisition_ID).select("APPROVAL_DT").rdd.map(lambda row : row[0].strftime('%m/%d/%Y')).collect(),
                          Last_Change_Date = sqlDF.filter(sqlDF["REQ_ID"] == Requisition_ID).select("LAST_DTTM_UPDATE").rdd.map(lambda row : row[0].strftime('%m/%d/%Y')).collect(),
                          Hold_From_Further_Processing = sqlDF.filter(sqlDF["REQ_ID"] == Requisition_ID).select("HOLD_STATUS").rdd.map(lambda row : row[0].encode("ascii", "ignore")).collect(),
                          Origin = sqlDF.filter(sqlDF["REQ_ID"] == Requisition_ID).select("ORIGIN").rdd.map(lambda row : row[0].encode("ascii", "ignore")).collect(),
                          Requester = sqlDF.filter(sqlDF["REQ_ID"] == Requisition_ID).select("REQUESTOR_ID").rdd.map(lambda row : row[0].encode("ascii", "ignore")).collect(),
                          Buyer = sqlDF.filter(sqlDF["REQ_ID"] == Requisition_ID).select("BUYER").rdd.map(lambda row : row[0].encode("ascii", "ignore")).collect(),
                          Buyer_Assignment = sqlDF.filter(sqlDF["REQ_ID"] == Requisition_ID).select("BUYER_ASSIGN").rdd.map(lambda row : row[0].encode("ascii", "ignore")).collect(),
                          Hold_Req_Process = sqlDF.filter(sqlDF["REQ_ID"] == Requisition_ID).select("HOLD_REQ_PROCESS").rdd.map(lambda row : row[0].encode("ascii", "ignore")).collect(),
                          Out_to_Bid = sqlDF.filter(sqlDF["REQ_ID"] == Requisition_ID).select("OUT_TO_BID").rdd.map(lambda row : row[0].encode("ascii", "ignore")).collect(),
                          Req_Total = sqlDF.filter(sqlDF["REQ_ID"] == Requisition_ID).select("REQ_TOTAL").rdd.map(lambda row : row[0]).collect())


    def resolve_PO_LINE(self, info, Requisition_ID, Line_Number):
        return Schema_PO_LINE(Business_Unit = sqlDF.filter((sqlDF["REQ_ID"] == Requisition_ID) & (sqlDF["LINE_NBR"] == Line_Number)).select("BUSINESS_UNIT").rdd.map(lambda row : row[0].encode("ascii", "ignore")).collect(),
                      Requisition_ID = sqlDF.filter((sqlDF["REQ_ID"] == Requisition_ID) & (sqlDF["LINE_NBR"] == Line_Number)).select("REQ_ID").rdd.map(lambda row : row[0].encode("ascii", "ignore")).collect(),
                      Line_Number = sqlDF.filter((sqlDF["REQ_ID"] == Requisition_ID) & (sqlDF["LINE_NBR"] == Line_Number)).select("LINE_NBR").rdd.map(lambda row : row[0]).collect(),
                      Requisition_Date = sqlDF.filter((sqlDF["REQ_ID"] == Requisition_ID) & (sqlDF["LINE_NBR"] == Line_Number)).select("REQ_DT").rdd.map(lambda row : row[0].strftime('%m/%d/%Y')).collect(),
                      Date_of_Approval = sqlDF.filter((sqlDF["REQ_ID"] == Requisition_ID) & (sqlDF["LINE_NBR"] == Line_Number)).select("APPROVAL_DT").rdd.map(lambda row : row[0].strftime('%m/%d/%Y')).collect(),
                      Last_Change_Date = sqlDF.filter((sqlDF["REQ_ID"] == Requisition_ID) & (sqlDF["LINE_NBR"] == Line_Number)).select("LAST_DTTM_UPDATE").rdd.map(lambda row : row[0].strftime('%m/%d/%Y')).collect(),
                      Hold_From_Further_Processing = sqlDF.filter((sqlDF["REQ_ID"] == Requisition_ID) & (sqlDF["LINE_NBR"] == Line_Number)).select("HOLD_STATUS").rdd.map(lambda row : row[0].encode("ascii", "ignore")).collect(),
                      Origin = sqlDF.filter((sqlDF["REQ_ID"] == Requisition_ID) & (sqlDF["LINE_NBR"] == Line_Number)).select("ORIGIN").rdd.map(lambda row : row[0].encode("ascii", "ignore")).collect(),
                      Requester = sqlDF.filter((sqlDF["REQ_ID"] == Requisition_ID) & (sqlDF["LINE_NBR"] == Line_Number)).select("REQUESTOR_ID").rdd.map(lambda row : row[0].encode("ascii", "ignore")).collect(),
                      Buyer = sqlDF.filter((sqlDF["REQ_ID"] == Requisition_ID) & (sqlDF["LINE_NBR"] == Line_Number)).select("BUYER").rdd.map(lambda row : row[0].encode("ascii", "ignore")).collect(),
                      Buyer_Assignment = sqlDF.filter((sqlDF["REQ_ID"] == Requisition_ID) & (sqlDF["LINE_NBR"] == Line_Number)).select("BUYER_ASSIGN").rdd.map(lambda row : row[0].encode("ascii", "ignore")).collect(),
                      Hold_Req_Process = sqlDF.filter((sqlDF["REQ_ID"] == Requisition_ID) & (sqlDF["LINE_NBR"] == Line_Number)).select("HOLD_REQ_PROCESS").rdd.map(lambda row : row[0].encode("ascii", "ignore")).collect(),
                      Out_to_Bid = sqlDF.filter((sqlDF["REQ_ID"] == Requisition_ID) & (sqlDF["LINE_NBR"] == Line_Number)).select("OUT_TO_BID").rdd.map(lambda row : row[0].encode("ascii", "ignore")).collect(),
                      Req_Total = sqlDF.filter((sqlDF["REQ_ID"] == Requisition_ID) & (sqlDF["LINE_NBR"] == Line_Number)).select("REQ_TOTAL").rdd.map(lambda row : row[0]).collect())


# Load schema on Graphql API:
schema=graphene.Schema(query=Query)

app = Flask(__name__)
app.add_url_rule(
 '/',
 view_func=GraphQLView.as_view('graphql', schema=schema, graphiql=True)
)
app.run(ssl_context='adhoc', host='0.0.0.0')

