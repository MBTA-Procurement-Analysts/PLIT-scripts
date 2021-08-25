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
import deltaGQL
import json

# Check if spark session is already running or create new one
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.appName("DeltaTemplate").getOrCreate()

# Load latest data:
deltapath = os.getenv("RUBIXTAPEDELTAPATH")

# Load data for SLT_WF_PO_APPR_FULL_STEPS_V2
GQL_PO_AW = deltaGQL.load_latest_version(deltapath+"/GQL_PO_AW")
GQL_PO_APPR_HDR_VW = deltaGQL.load_latest_version(deltapath+"/GQL_PO_APPR_HDR_VW")
GQL_EOAW_STEPINST = deltaGQL.load_latest_version(deltapath+"/GQL_EOAW_STEPINST")
GQL_EOAW_USERINST = deltaGQL.load_latest_version(deltapath+"/GQL_EOAW_USERINST")
GQL_PSOPRDEFN = deltaGQL.load_latest_version(deltapath+"/GQL_PSOPRDEFN")
GQL_PO_LINE_DISTRIB = deltaGQL.load_latest_version(deltapath+"/GQL_PO_LINE_DISTRIB")
GQL_REQ_LN_DISTRIB = deltaGQL.load_latest_version(deltapath+"/GQL_REQ_LN_DISTRIB")
GQL_REQ_HDR = deltaGQL.load_latest_version(deltapath+"/GQL_REQ_HDR")
GQL_MB_REQ_HOLD_HDR = deltaGQL.load_latest_version(deltapath+"/GQL_MB_REQ_HOLD_HDR")
GQL_MB_REQ_HOLD_LN = deltaGQL.load_latest_version(deltapath+"/GQL_MB_REQ_HOLD_LN")
GQL_REQ_LINE = deltaGQL.load_latest_version(deltapath+"/GQL_REQ_LINE")
print("All the tables loaded successfully")

# Get list of all spark dfs:
alldfs = [var for var in dir() if isinstance(eval(var), pyspark.sql.dataframe.DataFrame)]

# Get count of records in each df:
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
# Ouery: SLT_WF_PO_APPR_FULL_STEPS_V2
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
sqlDF1 = spark.sql('''SELECT DISTINCT A.BUSINESS_UNIT, A.PO_ID AS PO_ID, B.PO_DT AS PO_DT, B.PO_STATUS AS PO_STATUS, D.OPRID AS OPRID, K.OPRDEFNDESC AS OPRDEFNDESC, from_unixtime(unix_timestamp(D.DTTM_CREATED, 'dd-MMM-yyyy hh:mm:ss'), 'MM/dd/yyyy HH:mm:ss') AS INITIATED_DATETIME, from_unixtime(unix_timestamp(D.EOAWDTTM_MODIFIED, 'dd-MMM-yyyy hh:mm:ss'), 'MM/dd/yyyy HH:mm:ss') AS APPROVAL_DATETIME, CAST((D.EOAWDTTM_MODIFIED - D.DTTM_CREATED) AS STRING) AS PENDING_DAYS, D.EOAWORIG_OPRID, D.EOAWSTEP_STATUS, D.EOAWSTEP_INSTANCE, A.EOAWTHREAD_ID, C.EOAWPATH_ID, date_format(to_date(G.APPROVAL_DT, 'dd-MMM-yyyy hh:mm:ss'), 'MM/dd/yyyy') AS REQ_APPR_DATE, G.BUSINESS_UNIT, G.REQ_ID, G.HOLD_STATUS, H.BUYER_ID, I.LINE_NBR, I.MB_HOLD_TYPE, I.DUE_DT AS DUE_DT, I.COMMENTS60
  FROM GQL_PO_AW A, GQL_PO_APPR_HDR_VW B, GQL_EOAW_STEPINST C, GQL_EOAW_USERINST D, GQL_PSOPRDEFN K, GQL_PO_LINE_DISTRIB E, GQL_REQ_LN_DISTRIB F, GQL_REQ_HDR G, (GQL_MB_REQ_HOLD_HDR H LEFT OUTER JOIN  GQL_MB_REQ_HOLD_LN I ON  H.BUSINESS_UNIT = I.BUSINESS_UNIT AND H.REQ_ID = I.REQ_ID )
  WHERE ( B.BUSINESS_UNIT = A.BUSINESS_UNIT
     AND B.PO_ID = A.PO_ID
     AND A.EOAWTHREAD_ID = C.EOAWTHREAD_ID
     AND A.EOAWPRCS_ID = C.EOAWPRCS_ID
     AND C.EOAWDEFN_ID = A.EOAWDEFN_ID
     AND C.EOAWSTEP_INSTANCE = D.EOAWSTEP_INSTANCE
     AND K.OPRID = D.OPRID
     AND E.BUSINESS_UNIT = A.BUSINESS_UNIT
     AND E.PO_ID = A.PO_ID
     AND E.BUSINESS_UNIT = F.BUSINESS_UNIT
     AND E.LINE_NBR = F.LINE_NBR
     AND E.SCHED_NBR = F.SCHED_NBR
     AND E.DISTRIB_LINE_NUM = F.DISTRIB_LINE_NUM
     AND F.REQ_ID = E.REQ_ID
     AND F.BUSINESS_UNIT = G.BUSINESS_UNIT
     AND F.REQ_ID = G.REQ_ID
     AND G.BUSINESS_UNIT = H.BUSINESS_UNIT
     AND G.REQ_ID = H.REQ_ID)
  ORDER BY 2, 8, 12''')

# Query: SLT_REQS_ON_HOLD_HISTORY
sqlDF2 = spark.sql('''SELECT A.BUSINESS_UNIT AS BUSINESS_UNIT, A.REQ_ID AS REQ_ID, A.REQ_STATUS AS REQ_STATUS, A.HOLD_STATUS AS HOLD_STATUS, from_unixtime(unix_timestamp(A.APPROVAL_DT, 'dd-MMM-yyyy hh:mm:ss'), 'MM/dd/yyyy') AS APPROVAL_DATE, A.ORIGIN AS ORIGIN, A.OPRID_ENTERED_BY AS OPRID_ENTERED_BY, A.OPRID_MODIFIED_BY AS OPRID_MODIFIED_BY, from_unixtime(unix_timestamp(A.ENTERED_DT, 'dd-MMM-yyyy hh:mm:ss'), 'MM/dd/yyyy') AS REQ_ENTERED, from_unixtime(unix_timestamp(A.REQ_DT, 'dd-MMM-yyyy hh:mm:ss'), 'MM/dd/yyyy') AS REQ_DATE, D.LINE_NBR AS LINE_NBR, B.MB_HOLD_STATUS AS MB_HOLD_STATUS, B.BUYER_ID AS BUYER_ID, B.LAST_UPDATED_BY AS LAST_UPDATED_BY, C.LINE_NBR AS LINE_NBR, from_unixtime(unix_timestamp(C.DATETIME_STAMP, 'dd-MMM-yyyy hh:mm:ss'), 'MM/dd/yyyy HH:mm:ss') AS HOLD_LINE_DATETIME, from_unixtime(unix_timestamp(C.DUE_DT, 'dd-MMM-yyyy hh:mm:ss'), 'MM/dd/yyyy') AS DUE_DATE, C.MB_LINE_STATUS AS MB_LINE_STATUS, C.MB_HOLD_TYPE AS MB_HOLD_TYPE, C.COMMENTS60 AS COMMENTS60, F.BUSINESS_UNIT, F.PO_ID AS PO_ID, F.LINE_NBR
  FROM GQL_REQ_HDR A, GQL_MB_REQ_HOLD_HDR B, GQL_MB_REQ_HOLD_LN C, GQL_REQ_LINE D, (GQL_REQ_LN_DISTRIB E LEFT OUTER JOIN  GQL_PO_LINE_DISTRIB F ON  E.BUSINESS_UNIT = F.BUSINESS_UNIT AND E.REQ_ID = F.REQ_ID AND E.LINE_NBR = F.LINE_NBR AND E.SCHED_NBR = F.SCHED_NBR AND E.DISTRIB_LINE_NUM = F.DISTRIB_LINE_NUM ) 
  WHERE ( A.BUSINESS_UNIT = B.BUSINESS_UNIT 
     AND A.REQ_ID = B.REQ_ID 
     AND B.BUSINESS_UNIT = C.BUSINESS_UNIT 
     AND B.REQ_ID = C.REQ_ID 
     AND A.BUSINESS_UNIT = D.BUSINESS_UNIT 
     AND A.REQ_ID = D.REQ_ID 
     AND D.BUSINESS_UNIT = E.BUSINESS_UNIT 
     AND D.REQ_ID = E.REQ_ID 
     AND D.LINE_NBR = E.LINE_NBR 
     AND C.MB_LINE_STATUS IN ('C','N','X')) 
  ORDER BY 4 DESC, 5 DESC, 2, 15''')

# Stored data in cache:
sqlDF1.persist()
sqlDF2.persist()
print("Data loaded in cache")

# Get total number of records in query:
print("Total records in SLT_WF_PO_APPR_FULL_STEPS_V2: {}".format(sqlDF1.count()))
print("Total records in SLT_REQS_ON_HOLD_HISTORY: {}".format(sqlDF2.count()))

# Test df output:
#sqlDF2.select("BUSINESS_UNIT", "APPROVAL_DATE", "REQ_ENTERED", "REQ_DATE", "HOLD_LINE_DATETIME", "DUE_DATE").show(5, False)

# Convert spark dataframe to JSON:
sqlDF1 = sqlDF1.toJSON().map(lambda j: json.loads(j))
sqlDF2 = sqlDF2.toJSON().map(lambda j: json.loads(j))

# Schema for GraphQL API:
class Schema_PO_WF_APPR(graphene.ObjectType):
    Unit = graphene.String()
    PO_No = graphene.String()
    PO_Date = graphene.String()
    PO_Status = graphene.String()
    User = graphene.String()
    OPERID_Decoded = graphene.String()
    Initiated_Datetime = graphene.String()
    Approval_Datetime = graphene.String()
    Pending_Days = graphene.String()
    Step_User_ID = graphene.String()
    Step_Status = graphene.String()
    Step_Instance = graphene.String()
    Thread_ID = graphene.String()
    Path = graphene.String()
    REQ_APPR_Date = graphene.String()
    Req_ID = graphene.String()
    Hold = graphene.String()
    Buyer = graphene.String()
    Line = graphene.String()
    Hold_Type = graphene.String()
    Due = graphene.String()
    Comments = graphene.String()

class Schema_REQS_ON_HOLD_HISTORY(graphene.ObjectType):
    Unit = graphene.String()
    Req_ID = graphene.String()
    Requisition_Status = graphene.String()
    Hold = graphene.String()
    Approval_Date = graphene.String()
    Origin = graphene.String()
    Entered_By = graphene.String()
    User_Modify = graphene.String()
    Req_Entered = graphene.String()
    Req_Date = graphene.String()
    Req_Line = graphene.String()
    MB_Hold_Status = graphene.String()
    Buyer = graphene.String()
    Updated_By = graphene.String()
    Hold_Line = graphene.String()
    Hold_Line_DateTime = graphene.String()
    Due = graphene.String()
    Hold_Line_Status = graphene.String()
    Hold_Type = graphene.String()
    Comments = graphene.String()
    PO_No = graphene.String()
    PO_Line = graphene.String()

# Function for GraphQL:
class Query(graphene.ObjectType):
    PO_WF_APPR = graphene.List(Schema_PO_WF_APPR)
    REQS_ON_HOLD_HISTORY = graphene.List(Schema_REQS_ON_HOLD_HISTORY)

    def resolve_PO_WF_APPR(self, info):
        json_list = []
        for each_dict in sqlDF1.take(sqlDF1.count()):
            Unit = each_dict.get('BUSINESS_UNIT')
            PO_No = each_dict.get('PO_ID')
            PO_Date = each_dict.get('PO_DT')
            PO_Status = each_dict.get('PO_STATUS')
            User = each_dict.get('OPRID')
            OPERID_Decoded = each_dict.get('OPRDEFNDESC')
            Initiated_Datetime = each_dict.get('INITIATED_DATETIME')
            Approval_Datetime = each_dict.get('APPROVAL_DATETIME')
            Pending_Days = each_dict.get('PENDING_DAYS')
            Step_User_ID = each_dict.get('EOAWORIG_OPRID')
            Step_Status = each_dict.get('EOAWSTEP_STATUS')
            Step_Instance = each_dict.get('EOAWSTEP_INSTANCE')
            Thread_ID = each_dict.get('EOAWTHREAD_ID')
            Path = each_dict.get('EOAWPATH_ID')
            REQ_APPR_Date = each_dict.get('REQ_APPR_DATE')
            Req_ID = each_dict.get('REQ_ID')
            Hold = each_dict.get('HOLD_STATUS')
            Buyer = each_dict.get('BUYER_ID')
            Line = each_dict.get('LINE_NBR')
            Hold_Type = each_dict.get('MB_HOLD_TYPE')
            Due = each_dict.get('DUE_DT')
            Comments = each_dict.get('COMMENTS60')
            each_dict_schema = Schema_PO_WF_APPR(Unit, PO_No, PO_Date, PO_Status, User, OPERID_Decoded, Initiated_Datetime, Approval_Datetime, Pending_Days, Step_User_ID, Step_Status, Step_Instance, Thread_ID, Path, REQ_APPR_Date, Req_ID, Hold, Buyer, Line, Hold_Type, Due, Comments)
            json_list.append(each_dict_schema)
        return json_list


    def resolve_REQS_ON_HOLD_HISTORY(self, info):
        json_list = []
        for each_dict in sqlDF2.take(sqlDF2.count()):
            Unit = each_dict.get('BUSINESS_UNIT')
            Req_ID = each_dict.get('REQ_ID')
            Requisition_Status = each_dict.get('REQ_STATUS')
            Hold = each_dict.get('HOLD_STATUS')
            Approval_Date = each_dict.get('APPROVAL_DATE')
            Origin = each_dict.get('ORIGIN')
            Entered_By = each_dict.get('OPRID_ENTERED_BY')
            User_Modify = each_dict.get('OPRID_MODIFIED_BY')
            Req_Entered = each_dict.get('REQ_ENTERED')
            Req_Date = each_dict.get('REQ_DATE')
            Req_Line = each_dict.get('LINE_NBR')
            MB_Hold_Status = each_dict.get('MB_HOLD_STATUS')
            Buyer = each_dict.get('BUYER_ID')
            Updated_By = each_dict.get('LAST_UPDATED_BY')
            Hold_Line = each_dict.get('LINE_NBR')
            Hold_Line_DateTime = each_dict.get('HOLD_LINE_DATETIME')
            Due = each_dict.get('DUE_DATE')
            Hold_Line_Status = each_dict.get('MB_LINE_STATUS')
            Hold_Type = each_dict.get('MB_HOLD_TYPE')
            Comments = each_dict.get('COMMENTS60')
            PO_No = each_dict.get('PO_ID')
            PO_Line = each_dict.get('LINE_NBR')
            each_dict_schema = Schema_REQS_ON_HOLD_HISTORY( Unit, Req_ID, Requisition_Status, Hold, Approval_Date, Origin, Entered_By, User_Modify, Req_Entered, Req_Date, Req_Line, MB_Hold_Status, Buyer, Updated_By, Hold_Line, Hold_Line_DateTime, Due, Hold_Line_Status, Hold_Type, Comments, PO_No, PO_Line)
            json_list.append(each_dict_schema)
        return json_list
    
# Load schema on Graphql API:
schema=graphene.Schema(query=Query)

app = Flask(__name__)
app.add_url_rule(
 '/',
 view_func=GraphQLView.as_view('graphql', schema=schema, graphiql=True)
)
app.run(ssl_context='adhoc', host='0.0.0.0')
