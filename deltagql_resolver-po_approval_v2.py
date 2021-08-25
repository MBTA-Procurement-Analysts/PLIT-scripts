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
import deltaGQL as dl
import json

# Check if spark session is already running or create new one
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.appName("DeltaTemplate").getOrCreate()

# Load latest versions of all the required tables:
tables_required = ["GQL_PO_AW", "GQL_PO_APPR_HDR_VW", "GQL_EOAW_STEPINST", "GQL_EOAW_USERINST", "GQL_PSOPRDEFN", "GQL_PO_LINE_DISTRIB", "GQL_REQ_LN_DISTRIB", "GQL_REQ_HDR", "GQL_MB_REQ_HOLD_HDR", "GQL_MB_REQ_HOLD_LN", "GQL_REQ_LINE"]

for tbl in tables_required:
    exec('{} = dl.load_latest_version("{}")'.format(tbl, os.getenv("RUBIXTAPEDELTAPATH")+"/"+tbl))
print("All the tables loaded successfully.")

# Get list of all spark dfs:
alldfs = [var for var in dir() if isinstance(eval(var), pyspark.sql.dataframe.DataFrame)]

# Get number of records in each df:
count = [eval(df).count() for df in alldfs]
print("List of all the tables and number of records in them: \n{}".format(list(zip(alldfs, count))))

# Create views:
views = [(eval(df).createOrReplaceTempView(df), df) for df in alldfs]
print("List of all the views created: \n{}".format([df[1] for df in views]))

# Run SQL:
# Ouery: SLT_WF_PO_APPR_FULL_STEPS_V2
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
sqlDF1 = spark.sql(dl.sql_queries.get('SLT_WF_PO_APPR_FULL_STEPS_V2'))
sqlDF2 = spark.sql(dl.sql_queries.get('SLT_REQS_ON_HOLD_HISTORY'))

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
print("SQL output converted to JSON")

# Schema for GraphQL API:
Schema_PO_WF_APPR = dl.Schema_PO_WF_APPR
Schema_REQS_ON_HOLD_HISTORY = dl.Schema_REQS_ON_HOLD_HISTORY

# Function for GraphQL:
Query = dl.Query

# Load schema on Graphql API:
schema=graphene.Schema(query=Query)

app = Flask(__name__)
app.add_url_rule(
 '/',
 view_func=GraphQLView.as_view('graphql', schema=schema, graphiql=True)
)
app.run(ssl_context='adhoc', host='0.0.0.0')