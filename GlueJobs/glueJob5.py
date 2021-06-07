import sys
from awsglue.transforms import ApplyMapping
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext


# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


'''
Load data through the glueContext
'''
# @args: [database = "gluedatabase", table_name = "sales_salesorderheader_csv", transformation_ctx = "datasource0"]
# @return: sales_salesorderheader_glue_dynamic_frame
# @inputs: []
sales_salesorderheader_glue_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="gluedatabase", table_name="sales_salesorderheader_csv", transformation_ctx="datasource0")

'''
Mapping data to glueContext . 
'''
# @type: ApplyMapping
# @args: [mapping = [("salesorderid", "long", "salesorderid", "long"), ("revisionnumber", "long", "revisionnumber", "long"), ("orderdate", "string", "orderdate", "string"), ("duedate", "string", "duedate", "string"), ("shipdate", "string", "shipdate", "string"), ("status", "long", "status", "long"), ("onlineorderflag", "long", "onlineorderflag", "long"), ("salesordernumber", "string", "salesordernumber", "string"), ("purchaseordernumber", "string", "purchaseordernumber", "string"), ("accountnumber", "string", "accountnumber", "string"), ("customerid", "long", "customerid", "long"), ("salespersonid", "long", "salespersonid", "long"), ("territoryid", "long", "territoryid", "long"), ("billtoaddressid", "long", "billtoaddressid", "long"), ("shiptoaddressid", "long", "shiptoaddressid", "long"), ("shipmethodid", "long", "shipmethodid", "long"), ("creditcardid", "long", "creditcardid", "long"), ("creditcardapprovalcode", "string", "creditcardapprovalcode", "string"), ("currencyrateid", "long", "currencyrateid", "long"), ("subtotal", "string", "subtotal", "string"), ("taxamt", "string", "taxamt", "string"), ("freight", "string", "freight", "string"), ("totaldue", "string", "totaldue", "string"), ("comment", "string", "comment", "string"), ("rowguid", "string", "rowguid", "string"), ("modifieddate", "string", "modifieddate", "string")], transformation_ctx = "applymapping1"]
# @return: applymapping1
# @inputs: [frame = datasource0]
sales_salesorderheader_glue_dynamic_frame_mapping = ApplyMapping.apply(frame=sales_salesorderheader_glue_dynamic_frame, mappings=[("salesorderid", "long", "salesorderid", "long"), ("revisionnumber", "long", "revisionnumber", "long"), ("orderdate", "string", "orderdate", "string"), ("duedate", "string", "duedate", "string"), ("shipdate", "string", "shipdate", "string"), ("status", "long", "status", "long"), ("onlineorderflag", "long", "onlineorderflag", "long"), ("salesordernumber", "string", "salesordernumber", "string"), ("purchaseordernumber", "string", "purchaseordernumber", "string"), ("accountnumber", "string", "accountnumber", "string"), ("customerid", "long", "customerid", "long"), ("salespersonid", "long", "salespersonid", "long"), (
    "territoryid", "long", "territoryid", "long"), ("billtoaddressid", "long", "billtoaddressid", "long"), ("shiptoaddressid", "long", "shiptoaddressid", "long"), ("shipmethodid", "long", "shipmethodid", "long"), ("creditcardid", "long", "creditcardid", "long"), ("creditcardapprovalcode", "string", "creditcardapprovalcode", "string"), ("currencyrateid", "long", "currencyrateid", "long"), ("subtotal", "string", "subtotal", "string"), ("taxamt", "string", "taxamt", "string"), ("freight", "string", "freight", "string"), ("totaldue", "string", "totaldue", "string"), ("comment", "string", "comment", "string"), ("rowguid", "string", "rowguid", "string"), ("modifieddate", "string", "modifieddate", "string")], transformation_ctx="applymapping1")

'''
Convert dynamic frame to data frame . 
'''
sales_salesorderheader_glue_df = sales_salesorderheader_glue_dynamic_frame_mapping.toDF()

'''
Creation of Temp view .
'''
sales_salesorderheader_glue_df.createOrReplaceTempView('salesOrderTable')

'''
Query to fetch SalesOrderID, OrderDate , TotalDue in September 2011 with totalDueDate more than 1,000  .
'''

query = '''
    SELECT SalesOrderID, OrderDate , TotalDue
    FROM salesOrderTable
    WHERE OrderDate >= '2011-09-01 00:00:00.000'  AND OrderDate < '2011-10-01 00:00:00.000'
        AND TotalDue >= '1,000'
    ORDER BY TotalDue DESC
'''

query_result = spark.sql(query)

# Convert back to dynamic frame
dynamic_frame_write = DynamicFrame.fromDF(
    query_result, glueContext, "dynamic_frame_write")

repartitioned_data_frame = dynamic_frame_write.repartition(1)

datasink2 = glueContext.write_dynamic_frame.from_options(frame=repartitioned_data_frame, connection_type="s3", connection_options={
                                                         "path": "s3://gluejobtasksoutput/write/Task-5/"}, format="json", transformation_ctx="datasink2")


job.commit()
