# Databricks notebook source
import datetime, fnmatch
import pandas as pd
import json
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BinaryType, BooleanType, DateType, DoubleType
from pyspark.sql.functions import length,substring,md5, concat, col, lit, lower, upper, max, min, monotonically_increasing_id, regexp_replace, when, current_timestamp, to_date, broadcast, concat_ws, date_format, coalesce, greatest, from_unixtime, unix_timestamp, to_timestamp
from pyspark.sql.types import *
from pyspark.sql import functions as F
from multiprocessing.pool import ThreadPool
from delta.tables import DeltaTable
from pandas.io.json import json_normalize
from simple_salesforce import Salesforce
from pyspark.sql.window import *
from datetime import datetime

# COMMAND ----------

startTime = datetime.now()
startTime = startTime.strftime("%Y-%m-%dT%H:%M:%SZ")

# COMMAND ----------

tgtEnv = dbutils.widgets.get("tgtEnv").lower()
## Set environment variables ##  
if tgtEnv == "qa" or tgtEnv == "dev":
  configFileLocation = "dbfs:/mnt/{}/job-configs/salesops/Openprise_Config.json".format(tgtEnv)
  jobDB = "{}_odin_job_tables".format(tgtEnv)
  dbPrefix = "{}_odin".format(tgtEnv)
  bronzeDB = "{}_bronze".format(dbPrefix)
  snowflakepassword =  dbutils.secrets.get(scope="odin-ingest-{}".format(tgtEnv), key="password")
  snowflakeuser =  dbutils.secrets.get(scope="odin-ingest-{}".format(tgtEnv), key="username")
  salesforceDomain = "test"
  sfEnv = "https://test.salesforce.com"
  svcId = '0050L000009s6XdQAI'
  salesforceusername = dbutils.secrets.get(scope="fs-salesforce", key="qa-username")
  salesforcepassword = dbutils.secrets.get(scope="fs-salesforce", key="qa-password")
  salesforcetoken = dbutils.secrets.get(scope="fs-salesforce", key="qa-token")
  sf = Salesforce(username = salesforceusername, password = salesforcepassword, security_token = salesforcetoken, domain = salesforceDomain) 
  options = {
    "sfUrl" : "fsinvestments.us-east-1.snowflakecomputing.com",
    "sfUser" : snowflakeuser,
    "sfPassword" : snowflakepassword,
    "sfDatabase" : "ODIN_{}".format(tgtEnv),
    "sfSchema" : "JOB_METRICS",
    "sfWarehouse" : "REFINERY_INGEST_{}".format(tgtEnv),
    "sfRole" : "ODIN_SVC_INGEST_{}".format(tgtEnv),
    "on_error" : "continue",
  }

else:
  configFileLocation = "dbfs:/mnt/{salesops/job-configs/salesops/Openprise_Config.json"
  jobDB = "odin_job_tables"
  dbPrefix = "odin_{}".format(tgtEnv)
  bronzeDB = "{}_bronze".format(dbPrefix)
  snowflakepassword =  dbutils.secrets.get(scope="odin-ingest", key="password")
  snowflakeuser =  dbutils.secrets.get(scope="odin-ingest", key="username")
  salesforceDomain = "production"
  sfEnv = "https://login.salesforce.com"
  svcId = '0050L000009s6XdQAI'
  salesforceusername = dbutils.secrets.get(scope="fs-salesforce", key="production-username")
  salesforcepassword = dbutils.secrets.get(scope="fs-salesforce", key="production-password")
  salesforcetoken = dbutils.secrets.get(scope="fs-salesforce", key="production-token")
  sf = Salesforce(username = salesforceusername, password = salesforcepassword, security_token = salesforcetoken) 
  options = {
    "sfUrl" : "fsinvestments.us-east-1.snowflakecomputing.com",
    "sfUser" : snowflakeuser,
    "sfPassword" : snowflakepassword,
    "sfDatabase" : "ODIN",
    "sfSchema" : "JOB_METRICS",
    "sfWarehouse" : "REFINERY_INGEST_PROD",
    "sfRole" : "ODIN_SVC_INGEST_PROD",
    "on_error" : "continue",
  }
  
if tgtEnv == "qa" or tgtEnv == "dev":
  addressList = ["DataStrategy@fsinvestments.com"]
else:
  addressList = ["DataStrategy@fsinvestments.com"]

# COMMAND ----------

## Creates exception df with exceptions schema ##
all_exceptions_schema = StructType([StructField("FS_EXCEPTION_ID", StringType()),
                                    StructField("FS_POST_ID", StringType()),
                                    StructField("CASE_MANAGEMENT_ID", StringType()),
                                    StructField("SUBJECT", StringType()),
                                    StructField("DESCRIPTION", StringType()),
                                    StructField("RECORD_TYPE_ID", StringType()),
                                    StructField("OWNER_ID", StringType()),
                                    StructField("PIPELINE", StringType()),
                                    StructField("ORIGIN", StringType()),
                                    StructField("EXCEPTION_ID_TYPE", StringType()),
                                    StructField("EXCEPTION_SOURCE", StringType()),
                                    StructField("EXCEPTION_STATUS", StringType()),
                                    StructField("PRIORITY", StringType()),
                                    StructField("EXCEPTION_TYPE", StringType()),
                                    StructField("EXCEPTION_SUB_TYPE", StringType()),
                                    StructField("SOURCE_LINK", StringType()),
                                    StructField("FILE_CREATION_DTS", TimestampType()),
                                    StructField("EXCEPTION_DTS", TimestampType())])
all_exceptions = spark.createDataFrame([], schema=all_exceptions_schema)

metrics_df_schema = StructType([StructField("PROCESSING_DATE", DateType()),
                                    StructField("FAILED", BooleanType()),
                                    StructField("PIPELINE", StringType()),
                                    StructField("REFINERY_PHASE", StringType()),
                                    StructField("DATA_SOURCE", StringType()),
                                    StructField("TABLENAME", StringType()),
                                    StructField("FILENAME", StringType()),
                                    StructField("PROCESSING_TIME", TimestampType()),
                                    StructField("RECORDS_READ", DoubleType()),
                                    StructField("DUPLICATE_RECORDS", DoubleType()),
                                    StructField("QUARANTINED_RECORDS", DoubleType()),
                                    StructField("EXCEPTIONS", DoubleType()),
                                    StructField("RECORDS_WRITTEN", DoubleType())])
metricsDf = spark.createDataFrame([], schema=metrics_df_schema)

maxDtDfUnionSchema = StructType([StructField("jobId", StringType()),
                                    StructField("maxDtMetrics", TimestampType())])
maxDtDfUnion = spark.createDataFrame([], schema=maxDtDfUnionSchema)

# COMMAND ----------

# MAGIC %run ./OPENPRISE_FUNCTIONS

# COMMAND ----------

# MAGIC %run ../../odin_utilities/notebooks/utilities/SALESFORCE_CASE_v3

# COMMAND ----------

# MAGIC %run ../../odin_utilities/notebooks/utilities/SALESFORCE_CONNECTION

# COMMAND ----------

# MAGIC %run ../../odin_utilities/notebooks/utilities/APPEND_TO_METRICS

# COMMAND ----------

# MAGIC %run ../../odin_utilities/notebooks/utilities/ODIN_UTILS

# COMMAND ----------

# MAGIC %run ../../odin_utilities/notebooks/utilities/EMAIL

# COMMAND ----------

jobConfig = readInputMap(configFileLocation, 'df')
# jobConfig = spark.createDataFrame(["actchr_cr", "conchr_cr","aixlog","ai_insight_leads_asof","riaweb_participants"], "string").toDF("jobId")
try:
  maxDtDf = generateOpenPriseSOW(options, jobConfig, bronzeDB)
  maxDtDf.cache().count()
  display(maxDtDf)
except Exception as e:
  exceptionVariables = {"FS_POST_ID":"00030", 
                    "ORIGIN":"SOW CREATION",
                    "EXCEPTION_ID_TYPE":"OPENPRISE SOW", 
                    "FILE_CREATION_DTS":None, 
                    "FOR_SUBJECT":None, 
                    "FOR_BODY":str(e)}
  all_exceptions_insert = generateExceptionDf(exceptionVariables, tgtEnv, all_exceptions = all_exceptions)
  insertIntoExceptionsTbl(all_exceptions_insert, tgtEnv)

# COMMAND ----------

## ACTCHR_CR ##
if not maxDtDf.where("jobId == 'actchr_cr'").rdd.isEmpty():
  try:
    actchrLastLoadDts = maxDtDf.where("jobId = 'actchr_cr'").select("maxDtMetrics").collect()[0][0]
    actchrLastLoadDts = '1900-01-01T00:00:00.000+0000'
    achchrDf = generateACTCHR_CR(bronzeDB, actchrLastLoadDts)
    bulkUpsertToSalesforce(sfEnv, salesforceusername, salesforcepassword, salesforcetoken, "Accounting__c", achchrDf)
    eventRecordsDf = updateEventAccountingID(sfEnv, salesforceusername, salesforcepassword, salesforcetoken, startTime, svcId)
    bulkUpsertToSalesforce(sfEnv, salesforceusername, salesforcepassword, salesforcetoken, "Event", eventRecordsDf)
    achMetrics = generateMetrics(metricsDf, "actchr_cr", startTime, options, tgtEnv, bronzeDB, svcId, "Accounting__c", salesforceusername, salesforcepassword, salesforcetoken, sfEnv, achchrDf.count())
    appendToMetricsTbl(tgtEnv, achMetrics)
  except Exception as e:
    exceptionVariables = {"FS_POST_ID":"00031", 
                      "ORIGIN":"actchr_cr",
                      "EXCEPTION_ID_TYPE":"GENERATE ACTCHR_CR", 
                      "FILE_CREATION_DTS":None, 
                      "FOR_SUBJECT":None, 
                      "FOR_BODY":str(e)}
    all_exceptions_insert = generateExceptionDf(exceptionVariables, tgtEnv, all_exceptions = all_exceptions)
    insertIntoExceptionsTbl(all_exceptions_insert, tgtEnv)
else:
  print("No new ACTCHR_CR records to process")

# COMMAND ----------

## CONCHR_CR ##
if not maxDtDf.where("jobId == 'conchr_cr'").rdd.isEmpty():
  try:
    conchrLastLoadDts = maxDtDf.where("jobId = 'conchr_cr'").select("maxDtMetrics").collect()[0][0]
    conchrDf = generateCONCHR_CR(bronzeDB, conchrLastLoadDts)
    bulkUpsertToSalesforce(sfEnv, salesforceusername, salesforcepassword, salesforcetoken, "Accounting__c", conchrDf)
    conMetrics = generateMetrics(metricsDf, "conchr_cr", startTime, options, tgtEnv, bronzeDB, svcId, "Accounting__c", salesforceusername, salesforcepassword, salesforcetoken, sfEnv, conchrDf.count())
    appendToMetricsTbl(tgtEnv, conMetrics)
  except Exception as e:
    exceptionVariables = {"FS_POST_ID":"00032", 
                      "ORIGIN":"conchr_cr",
                      "EXCEPTION_ID_TYPE":"GENERATE CONCHR_CR", 
                      "FILE_CREATION_DTS":None, 
                      "FOR_SUBJECT":None, 
                      "FOR_BODY":str(e)}
    all_exceptions_insert = generateExceptionDf(exceptionVariables, tgtEnv, all_exceptions = all_exceptions)
    insertIntoExceptionsTbl(all_exceptions_insert, tgtEnv)
else:
  print("No new CONCHR_CR records to process")

# COMMAND ----------

## AIXLOG ##
if not maxDtDf.where("jobId == 'aixlog'").rdd.isEmpty():
  try:
    aixlogLastLoadDts = maxDtDf.where("jobId = 'aixlog'").select("maxDtMetrics").collect()[0][0]
    aixlogDf = generateAIXLOG(bronzeDB, aixlogLastLoadDts)
    bulkUpsertToSalesforce(sfEnv, salesforceusername, salesforcepassword, salesforcetoken, "AIX_Login__c", aixlogDf)
    aixMetrics = generateMetrics(metricsDf, "aixlog", startTime, options, tgtEnv, bronzeDB, svcId, "AIX_Login__c", salesforceusername, salesforcepassword, salesforcetoken, sfEnv, aixlogDf.count())
    appendToMetricsTbl(tgtEnv, aixMetrics)
  except Exception as e:
    exceptionVariables = {"FS_POST_ID":"00032", 
                      "ORIGIN":"conchr_cr",
                      "EXCEPTION_ID_TYPE":"GENERATE CONCHR_CR", 
                      "FILE_CREATION_DTS":None, 
                      "FOR_SUBJECT":None, 
                      "FOR_BODY":str(e)}
    all_exceptions_insert = generateExceptionDf(exceptionVariables, tgtEnv, all_exceptions = all_exceptions)
    insertIntoExceptionsTbl(all_exceptions_insert, tgtEnv)
else:
  print("No new AIXLOG records to process")

# COMMAND ----------

## AI Insight ##
if not maxDtDf.where("jobId == 'ai_insight_leads_asof'").rdd.isEmpty():
  try:
    aiInsightLastLoadDts = maxDtDf.where("jobId = 'ai_insight_leads_asof'").select("maxDtMetrics").collect()[0][0]
    aiInsightDf = generateAIInsight(bronzeDB, aiInsightLastLoadDts)
    bulkUpsertToSalesforce(sfEnv, salesforceusername, salesforcepassword, salesforcetoken, "AI_Insight__c", aiInsightDf)
    aiInsightMetrics = generateMetrics(metricsDf, "ai_insight_leads_asof", startTime, options, tgtEnv, bronzeDB, svcId, "AI_Insight__c", salesforceusername, salesforcepassword, salesforcetoken, sfEnv, aiInsightDf.count())
    appendToMetricsTbl(tgtEnv, aiInsightMetrics)
  except Exception as e:
    exceptionVariables = {"FS_POST_ID":"00032", 
                      "ORIGIN":"conchr_cr",
                      "EXCEPTION_ID_TYPE":"GENERATE CONCHR_CR", 
                      "FILE_CREATION_DTS":None, 
                      "FOR_SUBJECT":None, 
                      "FOR_BODY":str(e)}
    all_exceptions_insert = generateExceptionDf(exceptionVariables, tgtEnv, all_exceptions = all_exceptions)
    insertIntoExceptionsTbl(all_exceptions_insert, tgtEnv)
else:
  print("No new AI Insight records to process")

# COMMAND ----------

# ## List Management ##
# if not maxDtDf.where("jobId == 'riaweb_participants'").rdd.isEmpty():
#   try:
#     riaWebinarLastLoadDts = maxDtDf.where("jobId = 'riaweb_participants'").select("maxDtMetrics").collect()[0][0]
#     riaWebinar = generateRIAWebinar(bronzeDB, riaWebinarLastLoadDts)
#     bulkUpsertToSalesforce(sfEnv, salesforceusername, salesforcepassword, salesforcetoken, "Contact", riaWebinar.contactCreateDf)
#     bulkUpsertToSalesforce(sfEnv, salesforceusername, salesforcepassword, salesforcetoken, "Lead", riaWebinar.newLeadDf)
#     campaignAddDf = appendNewContactsToCampaign(sfEnv, salesforceusername, salesforcepassword, salesforcetoken, riaWebinar.campaignAddDf, riaWebinar.newRecordsDf, svcId, startTime)
#     bulkUpsertToSalesforce(sfEnv, salesforceusername, salesforcepassword, salesforcetoken, "CampaignMember", campaignAddDf)
#     dupeList = []
#     for item in riaWebinar.dupeRecordsDf.join(riaWebinar.newRecordsDf,"recordHashkey","inner").collect():
#       dupeList.append(item["recordHashkey"] + ": " + item["FirstName"] + " " + item["LastName"] + "\n")
#     exceptionVariables = {"FS_POST_ID":"00033", 
#                   "ORIGIN":"riaweb_participants",
#                   "EXCEPTION_ID_TYPE":"RIA Web Dupes", 
#                   "FILE_CREATION_DTS":None, 
#                   "FOR_SUBJECT":None, 
#                   "FOR_BODY":dupeList}
#     all_exceptions_insert = generateExceptionDf(exceptionVariables, tgtEnv, all_exceptions = all_exceptions)
#     insertIntoExceptionsTbl(all_exceptions_insert, tgtEnv)
#     sendEmail("datastrategy@fsinvestments.com, karthik.kasala@fsinvestments.com", "Duplicate Records from RIA Webinar", str(dupeList))
#     riaWebinarMetrics = generateMetrics(metricsDf, "riaweb_participants", startTime, options, tgtEnv, bronzeDB, svcId, "CampaignMember", salesforceusername, salesforcepassword, salesforcetoken, sfEnv, riaWebinar.newRecordsDf.count())
#     appendToMetricsTbl(tgtEnv, riaWebinarMetrics)
#     riaWebinarMetrics = generateMetrics(metricsDf, "riaweb_participants", startTime, options, tgtEnv, bronzeDB, svcId, "Contact", salesforceusername, salesforcepassword, salesforcetoken, sfEnv, riaWebinar.newRecordsDf.count())
#     appendToMetricsTbl(tgtEnv, riaWebinarMetrics)
#     riaWebinarMetrics = generateMetrics(metricsDf, "riaweb_participants", startTime, options, tgtEnv, bronzeDB, svcId, "Lead", salesforceusername, salesforcepassword, salesforcetoken, sfEnv, riaWebinar.newRecordsDf.count())
#     appendToMetricsTbl(tgtEnv, riaWebinarMetrics)
#   except Exception as e:
#     exceptionVariables = {"FS_POST_ID":"00032", 
#                       "ORIGIN":"conchr_cr",
#                       "EXCEPTION_ID_TYPE":"GENERATE CONCHR_CR", 
#                       "FILE_CREATION_DTS":None, 
#                       "FOR_SUBJECT":None, 
#                       "FOR_BODY":str(e)}
#     all_exceptions_insert = generateExceptionDf(exceptionVariables, tgtEnv, all_exceptions = all_exceptions)
#     insertIntoExceptionsTbl(all_exceptions_insert, tgtEnv)
# else:
#   print("No new RIA Webinar records to process")