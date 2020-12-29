# Databricks notebook source
def readInputMap(fileLocation, output):
  """ pass filelocation (in json) and desired output from json, list, df object types"""
  fl = fileLocation
  if output == 'json':
    objInDF = spark.read.json(fl)
    objInRddJSON = objInDF.toJSON()
    item = objInRddJSON
  if output == 'list':
    objInDF = spark.read.json(fl)
    objInList = objInDF.toJSON().collect()
    item = objInList
  if output == 'df':
    objInDF = spark.read.json(fl)
    item = objInDF
  return item

# COMMAND ----------

def generateOpenPriseSOW(options, jobConfig, bronzeDB):
  
  maxDtDfUnion = spark.createDataFrame([], schema=maxDtDfUnionSchema)
  for job in jobConfig.collect():
    metricsMaxDtQuery = "select DATA_SOURCE as jobId, max(PROCESSING_DATE) as maxDtMetrics from refinery_metrics where refinery_phase = 'OPENPRISE' and DATA_SOURCE = '{job}' group by DATA_SOURCE".format(job = job["jobId"])
    metricsMaxDtDf = spark.read.format("net.snowflake.spark.snowflake") \
            .options(**options) \
            .option("query", metricsMaxDtQuery) \
            .load()
    deltaMaxDtDf = spark.sql("select '{job}' as jobId, max(lastloaddts) as maxDtDelta from {bronzeDB}.{job} group by '{job}'".format(job = job["jobId"], bronzeDB = bronzeDB))

    maxDtDf = deltaMaxDtDf.join(metricsMaxDtDf, "jobId", "full")

    if maxDtDfUnion.rdd.isEmpty(): 
      maxDtDfUnion = maxDtDf
    else:
      maxDtDfUnion = maxDtDfUnion.union(maxDtDf)

  maxDtDfUnion = maxDtDfUnion.withColumn("MAXDTMETRICS",when(maxDtDfUnion["MAXDTMETRICS"].isNull(),'1900-01-01T00:00:00.000+0000').otherwise(maxDtDfUnion["MAXDTMETRICS"]))
  maxDtDfUnion = maxDtDfUnion.where("maxDtDelta > maxDtMetrics")

  return maxDtDfUnion

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from dev_odin_bronze.conchr_cr

# COMMAND ----------

def generateACTCHR_CR(bronzeDB, maxLastLoadDts):
  
  mainRecordsDf = spark.sql(""" 
  select 
  '' as Id, 
  a.ActivitiesCode as Activities_Code__c, 
  case when a.ActivitiesCode like '701%' then a.ActivitiesCode else '' end as Campaign__c,
  case when a.ExpenseType like '%Gift%' then '0120L000000ReVq' else '0120L000000ReVpQAK' end as RecordTypeId,
  a.ExpenseType as Type__c,
  a.ReportNumber as Report_Number__c, 
  a.FileDerivedDate as Export_Date__c,
  a.FirstName as First_Name__c,
  a.LastName as Last_Name__c,
  a.Budget as Budget__c,
  a.AllocationDescription as Allocation_Description__c,
  a.AllocationCode as Allocation_Code__c,
  a.Amount as Amount__c
  from {bronzeDB}.actchr_cr a
  inner join (select ActivitiesCode, count(ActivitiesCode) as ACCodeCount from {bronzeDB}.actchr_cr where LastLoadDts > '{maxLastLoadDts}' group by ActivitiesCode) b
  on a.ActivitiesCode = b.ActivitiesCode
  where a.LastLoadDts > '{maxLastLoadDts}'
  """.format(bronzeDB = bronzeDB, maxLastLoadDts = maxLastLoadDts))

  aggRecordsDf = spark.sql("""
  select 
  '' as Id, 
  ActivitiesCode as Activities_Code__c, 
  '' as Campaign__c,
  '0120L000000ReVoQAK' as RecordTypeId,
  '' as Type__c,
  '' as Report_Number__c, 
  '' as Export_Date__c,
  '' as First_Name__c,
  '' as Last_Name__c,
  '' as Budget__c,
  '' as Allocation_Description__c,
  '' as Allocation_Code__c,
  '' as Amount__c from (
  select ActivitiesCode, count(ActivitiesCode) as ACCodeCount from {bronzeDB}.actchr_cr where LastLoadDts > '{maxLastLoadDts}' group by ActivitiesCode)
  where ACCodeCount > 1
  """.format(bronzeDB = bronzeDB, maxLastLoadDts = maxLastLoadDts))

  allRecordsDf = mainRecordsDf.union(aggRecordsDf)
  
  return allRecordsDf

# COMMAND ----------

def updateEventAccountingID(sfEnv, salesforceusername, salesforcepassword, salesforcetoken, startTime, svcId):

  soql = "select id, lastmodifiedbyid, recordtypeid, Activities_Code__c, systemmodstamp from Accounting__c where LastModifiedById = '{svcId}' and systemmodstamp > {startTime}".format(svcId = svcId, startTime = startTime)
  DF = spark.read.format("com.springml.spark.salesforce") \
                 .option("login", sfEnv) \
                 .option("username", salesforceusername) \
                 .option("password", salesforcepassword+salesforcetoken) \
                 .option("soql", soql) \
                 .option("pkChunking", True) \
                 .option("chunkSize", 50000) \
                 .load()
  accntngRecords = DF.where("RecordTypeId = '0120L000000ReVpQAK'")
  uniqueAccntngRecords = accntngRecords.groupBy("Activities_Code__c").count().filter("count = 1")
  accntngRecordsWithCount = accntngRecords.join(uniqueAccntngRecords,"Activities_Code__c","inner")
  accntngRecordsWithCount = accntngRecordsWithCount.withColumnRenamed("Id","Accounting_Item__c").withColumnRenamed("Activities_Code__c","Id")
  accntngRecordsWithCount = accntngRecordsWithCount.select("Id","Accounting_Item__c")
  accntngGroupRecords = DF.where("RecordTypeId = '0120L000000ReVoQAK'")
  accntngGroupRecords = accntngGroupRecords.withColumnRenamed("Id","Accounting_Item__c").withColumnRenamed("Activities_Code__c","Id")
  accntngGroupRecords = accntngGroupRecords.select("Id","Accounting_Item__c")
  accntngRecordsUnion = accntngGroupRecords.union(accntngRecordsWithCount)
  accntngRecordsUnion = accntngRecordsUnion.withColumn("Description",lit("ODIN UAT TEST"))

  return accntngRecordsUnion

# COMMAND ----------

def generateCONCHR_CR(bronzeDB, maxLastLoadDts):
  
  mainRecordsDf = spark.sql(""" 
  select 
  '' as Id, 
  a.SFContactID as Contact__c, 
  case when a.ActivityType like '%Gift%' then '0120L000000ReVq' else '0120L000000ReVpQAK' end as RecordTypeId,
  a.Type as Type__c,
  a.ReportID as Report_Number__c, 
  a.FileDerivedDate as Export_Date__c,
  a.Budget as Budget__c,
  a.Description as Allocation_Description__c,
  a.AllocationAmount as Amount__c
  from {bronzeDB}.conchr_cr a
  where LastLoadDts > '{maxLastLoadDts}'
  """.format(bronzeDB = bronzeDB, maxLastLoadDts = maxLastLoadDts))
  
  return mainRecordsDf

# COMMAND ----------

def generateAIXLOG(bronzeDB, maxLastLoadDts):
  
  df = spark.sql("""
  select 
  '' as id, 
  x.Email_Address__c, 
  x.Login_Date1__c, y.id as Contact__c, 
  y.AccountId as Account__c 
  from {bronzeDB}.aixlog x 
  inner join (select a.id, a.Email, a.AccountId from {bronzeDB}.contact a inner join (select id, max(FileDerivedDate) as FileDerivedDate from {bronzeDB}.contact group by id) b 
  on a.id = b.id 
  and a.FileDerivedDate = b.FileDerivedDate 
  where a.Email != 'null') y 
  on x.Email_Address__c = y.Email 
  where x.lastloaddts > '{maxLastLoadDts}'
  """.format(bronzeDB = bronzeDB, maxLastLoadDts = maxLastLoadDts))
  
  return df

# COMMAND ----------

def generateAIInsight(bronzeDB, maxLastLoadDts):
  
  df = spark.sql("""
  select 
  '' as id, 
  x.AdvisorCRD as CRD_Number__c,
  x.AdvisorEmail as Advisor_Email__c,
  y.id as Advisor_name__c,
  x.AdvisorRole as Advisor_Role__c,
  y.SFDC_Account_Name_Test__c as Broker_Dealer_Name__c,
  x.City as City__c,
  x.DateClosed as Date_Closed__c,
  date(to_timestamp(x.DateGenerated, 'mm/dd/yyyy')) as Date_Generated__c,
  x.State as State__c,
  x.Subject as Subject__c,
  x.ZipCode as Zip__c
  from {bronzeDB}.ai_insight_leads_asof x 
  inner join (select a.id, a.Email, a.CRD_Number__c, a.SFDC_Account_Name_Test__c from {bronzeDB}.contact a inner join (select id, max(FileDerivedDate) as FileDerivedDate from {bronzeDB}.contact group by id) b 
  on a.id = b.id 
  and a.FileDerivedDate = b.FileDerivedDate) y 
  on (x.AdvisorEmail = y.Email or x.AdvisorCRD = y.CRD_Number__c)
  where x.lastloaddts > '{maxLastLoadDts}'
  """.format(bronzeDB = bronzeDB, maxLastLoadDts = maxLastLoadDts))
  
  df = df.replace("Not Disclosed", None)
  df = df.withColumn("Date_Generated__c", date_format("Date_Generated__c", "yyyy-MM-dd'T'HH:mm:ssX"))
  
  return df

# COMMAND ----------

class generateRIAWebinar:
  def __init__(self, bronzeDB, maxLastLoadDts):

    ## Get new bronze records ##
    newRecordsDf = spark.sql("""
    select * from dev_odin_bronze.riaweb_participants
    where lastloaddts > '1900-01-01T00:00:00.000+0000'""")
    newRecordsDf.cache().count()
    
    ## Get all contact records ##
    contactsDf = spark.sql("""
    select a.id as ContactId, a.Email as RepEmail, a.CRD_Number__c as SFRepCRD from odin_salesops_bronze.contact a inner join (select id, max(FileDerivedDate) as FileDerivedDate from odin_salesops_bronze.contact group by id) b 
    on a.id = b.id 
    and a.FileDerivedDate = b.FileDerivedDate
    """)
    contactsDf.cache().count()
    
    ## Get all account records ##
    accountsDf = spark.sql("""
    select a.id as AccountId, a.Name as AccountName, a.CRD_Number__c as SFFirmCRD from odin_salesops_bronze.account a inner join (select id, max(FileDerivedDate) as FileDerivedDate from odin_salesops_bronze.account group by id) b 
    on a.id = b.id 
    and a.FileDerivedDate = b.FileDerivedDate
    """)
    accountsDf.cache().count()
    
    ## Get all zip records ##
    zipDf = spark.sql("""
    select a.id as ZipId, a.External_Id__c as ZipCode from odin_salesops_bronze.zip_code__c a inner join (select id, max(FileDerivedDate) as FileDerivedDate from odin_salesops_bronze.zip_code__c group by id) b 
    on a.id = b.id 
    and a.FileDerivedDate = b.FileDerivedDate
    """)
    zipDf.cache().count()

    ## Get all state records ##
    stateDf = spark.sql("""
    select a.id as StateId, a.Name as State from odin_salesops_bronze.state__c a inner join (select id, max(FileDerivedDate) as FileDerivedDate from odin_salesops_bronze.state__c group by id) b 
    on a.id = b.id 
    and a.FileDerivedDate = b.FileDerivedDate
    """)
    stateDf.cache().count()
    
    ## Match on Rep CRD and remove matches from records to process ##
    repCrdMatchDf = newRecordsDf.where("RegRepCRD is not null").join(contactsDf.where("SFRepCRD is not null"), newRecordsDf.RegRepCRD == contactsDf.SFRepCRD, "inner")
    uniqueRepCrdMatchDf = repCrdMatchDf.groupBy("recordHashkey").count().filter("count = 1")
    dupeRepCrdMatchDf = repCrdMatchDf.groupBy("recordHashkey").count().filter("count > 1")
    remainingNewRecordDf = newRecordsDf.join(repCrdMatchDf, "recordHashkey","leftanti")

    ## Match on Rep Email and remove matches from records to process ##
    repEmailMatchDf = remainingNewRecordDf.where("Email is not null").join(contactsDf.where("RepEmail is not null"), remainingNewRecordDf.Email == contactsDf.RepEmail, "inner")
    uniqueRepEmailMatchDf = repEmailMatchDf.groupBy("recordHashkey").count().filter("count = 1")
    dupeRepEmailMatchDf = repEmailMatchDf.groupBy("recordHashkey").count().filter("count > 1")
    remainingNewRecordDf = remainingNewRecordDf.join(repEmailMatchDf, "recordHashkey","leftanti")

    ## Match on Firm CRD and remove matches from records to process ##
    acctCrdMatchDf = remainingNewRecordDf.where("FirmCRD is not null").join(accountsDf.where("SFFirmCRD is not null"), remainingNewRecordDf.FirmCRD == accountsDf.SFFirmCRD, "inner")
    uniqueAcctCrdMatchDf = acctCrdMatchDf.groupBy("recordHashkey").count().filter("count = 1")
    dupeAcctCrdMatchDf = acctCrdMatchDf.groupBy("recordHashkey").count().filter("count > 1")
    remainingNewRecordDf = remainingNewRecordDf.join(acctCrdMatchDf, "recordHashkey","leftanti")

    ## Match on Firm Name and remove matches from records to process ##
    acctNameMatchDf = remainingNewRecordDf.where("Company is not null").join(accountsDf.where("AccountName is not null"), remainingNewRecordDf.Company == accountsDf.AccountName, "inner")
    uniqueAcctNameMatchDf = acctNameMatchDf.groupBy("recordHashkey").count().filter("count = 1")
    dupeAcctNameMatchDf = acctNameMatchDf.groupBy("recordHashkey").count().filter("count > 1")

    ## Union records for contact creation ##
    contactCreateDf = uniqueAcctCrdMatchDf.join(acctCrdMatchDf,"recordHashkey","inner").union(uniqueAcctNameMatchDf.join(acctNameMatchDf,"recordHashkey","inner")).join(zipDf, "ZipCode", "left").join(stateDf, "State", "left").selectExpr("'' as Id","StateId as State__c","AccountId as AccountId","Email as Email","SourceChannel as Source_Channel__c","FirstName as FirstName","Title as Title","RegRepCRD as CRD_Number__c","LastName as LastName","WorkPhone as Phone","ZipId as Zip_Code__c ")

    ## Union records for campaign addition ##
    campaignAddDf = repCrdMatchDf.join(uniqueRepCrdMatchDf, "recordHashkey","inner").selectExpr("CampaignID as CampaignId","ContactId").union(repEmailMatchDf.join(uniqueRepEmailMatchDf, "recordHashkey","inner").selectExpr("CampaignID as CampaignId","ContactId"))

    ## Get remaining records and create leads ##
    newLeadDf = remainingNewRecordDf.join(acctCrdMatchDf, "recordHashkey","leftanti").join(zipDf, "ZipCode", "left").join(stateDf, "State", "left").selectExpr("'' as Id","Company as Company","LastName as LastName","RegRepCRD as CRD_Number__c","Email as Email","FirstName as FirstName","WorkPhone as Phone","SourceChannel as Source_Channel__c","StateId as State","Title as Title","ZipId as Zip_Code_Lookup__c")

    ## Union dupe dfs for exception handling ##
    dupeRecordsDf = dupeRepCrdMatchDf.union(dupeRepEmailMatchDf).union(dupeAcctCrdMatchDf).union(dupeAcctNameMatchDf)
      
    self.newRecordsDf = newRecordsDf  
    self.contactCreateDf = contactCreateDf
    self.campaignAddDf = campaignAddDf
    self.newLeadDf = newLeadDf
    self.dupeRecordsDf = dupeRecordsDf

# COMMAND ----------

def appendNewContactsToCampaign(sfEnv, salesforceusername, salesforcepassword, salesforcetoken, campaignAddDf, newRecordsDf, svcId, startTime):
  
  soql = "select id, lastmodifiedbyid, recordtypeid, systemmodstamp, Email, FirstName, LastName, CRD_Number__c from Contact where LastModifiedById = '{svcId}' and systemmodstamp > {startTime}".format(svcId = svcId, startTime = startTime)
  newContactsDf = spark.read.format("com.springml.spark.salesforce") \
                 .option("login", sfEnv) \
                 .option("username", salesforceusername) \
                 .option("password", salesforcepassword+salesforcetoken) \
                 .option("soql", soql) \
                 .option("pkChunking", True) \
                 .option("chunkSize", 50000) \
                 .load()
  if not newContactsDf.rdd.isEmpty():
    newContactsDf = newContactsDf.join(newRecordsDf, (newContactsDf.FirstName == newRecordsDf.FirstName) & (newContactsDf.LastName == newRecordsDf.LastName), "inner").selectExpr("CampaignID as CampaignId", "Id as ContactId")
    campaignAddDf = campaignAddDf.union(newContactsDf)
  
  return campaignAddDf

# COMMAND ----------

def bulkUpsertToSalesforce(sfEnv, salesforceusername, salesforcepassword, salesforcetoken, object, df):

  test = df.write \
   .format("com.springml.spark.salesforce") \
   .option("login", sfEnv) \
   .option("username", salesforceusername) \
   .option("password", salesforcepassword+salesforcetoken) \
   .option("sfObject", object) \
   .option("upsert", True) \
   .option("bulk", True) \
   .save()
  print("Successfully wrote to " + object)
  
  return test

# COMMAND ----------

def generateMetrics(metricsDf, job, jobStartTime, options, tgtEnv, bronzeDB, svcId, obj, salesforceusername, salesforcepassword, salesforcetoken, sfEnv, readCnt):

## Records wrote from salesforce > jobstarttime
## Exceptions from snowflake > jobstarttime
  
  soql = "select count(LastModifiedById) from {obj} where LastModifiedById = '{svcId}' and systemmodstamp > {jobStartTime}".format(svcId = svcId, jobStartTime = jobStartTime, obj = obj)
  writeCnt = spark.read.format("com.springml.spark.salesforce") \
                 .option("login", sfEnv) \
                 .option("username", salesforceusername) \
                 .option("password", salesforcepassword+salesforcetoken) \
                 .option("soql", soql) \
                 .option("pkChunking", True) \
                 .option("chunkSize", 50000) \
                 .load()
  writeCnt = writeCnt.collect()[0][0]

  ## Pull exception count from snowflake ##
  exceptionCntQuery = "select count(*) as exceptionCnt from exceptions.refinery_exceptions where exception_source = 'ODIN Openprise' and exception_dts > '{jobStartTime}' and ORIGIN = '{job}'".format(jobStartTime = jobStartTime, job = job)
  exceptionCnt = spark.read.format("net.snowflake.spark.snowflake") \
                  .options(**options) \
                  .option("query", exceptionCntQuery) \
                  .load()
  if exceptionCnt.rdd.isEmpty():
    exceptionCnt = 0
  else:
    exceptionCnt = exceptionCnt.select("EXCEPTIONCNT").collect()[0][0]

#   for row in metricsCntJoin.collect():
  metricsDfNew = spark.sql("""
                      SELECT '{processingDts}' as PROCESSING_DATE,
                      False as FAILED,
                      '{tgtEnv}' AS PIPELINE,
                      'OPENPRISE' AS REFINERY_PHASE, 
                      '{job}' AS DATA_SOURCE, 
                      '{obj}' AS TABLENAME,
                      null AS FILENAME, 
                      null AS PROCESSING_TIME,
                      '{recordsRead}' AS RECORDS_READ,
                      '{dupeRecords}' AS DUPLICATE_RECORDS,
                      '{quarRecords}' AS QUARANTINED_RECORDS,
                      '{exceptions}' AS EXCEPTIONS,
                      '{recordsWritten}' AS RECORDS_WRITTEN
                      """.format(processingDts = jobStartTime,
                                 tgtEnv = tgtEnv,
                                 obj = obj,
                                 job = job,
                                 recordsRead = readCnt,
                                 dupeRecords = 0,
                                 quarRecords = 0,
                                 exceptions = exceptionCnt,
                                 recordsWritten = writeCnt
                                ))
  metricsDf = metricsDf.union(metricsDfNew)
  
  return metricsDf