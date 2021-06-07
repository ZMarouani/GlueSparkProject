import sys
from awsglue.transforms import ApplyMapping
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import concat_ws


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

# @args: [database = "gluedatabase", table_name = "sales_customer_csv", transformation_ctx = "datasource0"]
# @return: sales_customer_glue_dynamic_frame
# @inputs: []
sales_customer_glue_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="gluedatabase", table_name="sales_customer_csv", transformation_ctx="datasource0")

# @args: [database = "gluedatabase", table_name = "person_person_csv", transformation_ctx = "datasource0"]
# @return: person_person_glue_dynamic_frame
# @inputs: []
person_person_glue_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="gluedatabase", table_name="person_person_csv", transformation_ctx="datasource0")

# @args: [database = "gluedatabase", table_name = "sales_salesorderheader_csv", transformation_ctx = "datasource0"]
# @return: sales_salesorderheader_glue_dynamic_frame
# @inputs: []
sales_salesorderheader_glue_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="gluedatabase", table_name="sales_salesorderheader_csv", transformation_ctx="datasource0")


'''
Mapping data to glueContext . 
'''
# @type: ApplyMapping
# @args: [mapping = [("customerid", "long", "customerid", "long"), ("personid", "long", "personid", "long"), ("storeid", "long", "storeid", "long"), ("territoryid", "long", "territoryid", "long"), ("accountnumber", "string", "accountnumber", "string"), ("rowguid", "string", "rowguid", "string"), ("modifieddate", "string", "modifieddate", "string")], transformation_ctx = "applymapping1"]
# @return: applymapping1
# @inputs: [frame = datasource0]
sales_customer_glue_dynamic_frame_mapping = ApplyMapping.apply(frame=sales_customer_glue_dynamic_frame, mappings=[("customerid", "long", "customerid", "long"), ("personid", "long", "personid", "long"), ("storeid", "long", "storeid", "long"), (
    "territoryid", "long", "territoryid", "long"), ("accountnumber", "string", "accountnumber", "string"), ("rowguid", "string", "rowguid", "string"), ("modifieddate", "string", "modifieddate", "string")], transformation_ctx="applymapping1")

# @type: ApplyMapping
# @args: [mapping = [("businessentityid", "long", "businessentityid", "long"), ("persontype", "string", "persontype", "string"), ("namestyle", "long", "namestyle", "long"), ("title", "string", "title", "string"), ("firstname", "string", "firstname", "string"), ("middlename", "string", "middlename", "string"), ("lastname", "string", "lastname", "string"), ("suffix", "string", "suffix", "string"), ("emailpromotion", "long", "emailpromotion", "long"), ("additionalcontactinfo", "string", "additionalcontactinfo", "string"), ("demographics", "string", "demographics", "string"), ("rowguid", "string", "rowguid", "string"), ("modifieddate", "string", "modifieddate", "string")], transformation_ctx = "applymapping1"]
# @return: person_person_glue_dynamic_frame_mapping
# @inputs: [frame = datasource0]
person_person_glue_dynamic_frame_mapping = ApplyMapping.apply(frame=person_person_glue_dynamic_frame, mappings=[("businessentityid", "long", "businessentityid", "long"), ("persontype", "string", "persontype", "string"), ("namestyle", "long", "namestyle", "long"), ("title", "string", "title", "string"), ("firstname", "string", "firstname", "string"), ("middlename", "string", "middlename", "string"), (
    "lastname", "string", "lastname", "string"), ("suffix", "string", "suffix", "string"), ("emailpromotion", "long", "emailpromotion", "long"), ("additionalcontactinfo", "string", "additionalcontactinfo", "string"), ("demographics", "string", "demographics", "string"), ("rowguid", "string", "rowguid", "string"), ("modifieddate", "string", "modifieddate", "string")], transformation_ctx="applymapping1")

# @type: ApplyMapping
# @args: [mapping = [("salesorderid", "long", "salesorderid", "long"), ("revisionnumber", "long", "revisionnumber", "long"), ("orderdate", "string", "orderdate", "string"), ("duedate", "string", "duedate", "string"), ("shipdate", "string", "shipdate", "string"), ("status", "long", "status", "long"), ("onlineorderflag", "long", "onlineorderflag", "long"), ("salesordernumber", "string", "salesordernumber", "string"), ("purchaseordernumber", "string", "purchaseordernumber", "string"), ("accountnumber", "string", "accountnumber", "string"), ("customerid", "long", "customerid", "long"), ("salespersonid", "long", "salespersonid", "long"), ("territoryid", "long", "territoryid", "long"), ("billtoaddressid", "long", "billtoaddressid", "long"), ("shiptoaddressid", "long", "shiptoaddressid", "long"), ("shipmethodid", "long", "shipmethodid", "long"), ("creditcardid", "long", "creditcardid", "long"), ("creditcardapprovalcode", "string", "creditcardapprovalcode", "string"), ("currencyrateid", "long", "currencyrateid", "long"), ("subtotal", "string", "subtotal", "string"), ("taxamt", "string", "taxamt", "string"), ("freight", "string", "freight", "string"), ("totaldue", "string", "totaldue", "string"), ("comment", "string", "comment", "string"), ("rowguid", "string", "rowguid", "string"), ("modifieddate", "string", "modifieddate", "string")], transformation_ctx = "applymapping1"]
# @return: applymapping1
# @inputs: [frame = datasource0]
sales_salesorderheader_glue_dynamic_frame_mapping = ApplyMapping.apply(frame=sales_salesorderheader_glue_dynamic_frame, mappings=[("salesorderid", "long", "salesorderid", "long"), ("revisionnumber", "long", "revisionnumber", "long"), ("orderdate", "string", "orderdate", "string"), ("duedate", "string", "duedate", "string"), ("shipdate", "string", "shipdate", "string"), ("status", "long", "status", "long"), ("onlineorderflag", "long", "onlineorderflag", "long"), ("salesordernumber", "string", "salesordernumber", "string"), ("purchaseordernumber", "string", "purchaseordernumber", "string"), ("accountnumber", "string", "accountnumber", "string"), ("customerid", "long", "customerid", "long"), ("salespersonid", "long", "salespersonid", "long"), (
    "territoryid", "long", "territoryid", "long"), ("billtoaddressid", "long", "billtoaddressid", "long"), ("shiptoaddressid", "long", "shiptoaddressid", "long"), ("shipmethodid", "long", "shipmethodid", "long"), ("creditcardid", "long", "creditcardid", "long"), ("creditcardapprovalcode", "string", "creditcardapprovalcode", "string"), ("currencyrateid", "long", "currencyrateid", "long"), ("subtotal", "string", "subtotal", "string"), ("taxamt", "string", "taxamt", "string"), ("freight", "string", "freight", "string"), ("totaldue", "string", "totaldue", "string"), ("comment", "string", "comment", "string"), ("rowguid", "string", "rowguid", "string"), ("modifieddate", "string", "modifieddate", "string")], transformation_ctx="applymapping1")


'''
Convert dynamic frame to data frame . 
'''
sales_customer_df = sales_customer_glue_dynamic_frame_mapping.toDF()
person_person_glue_df = person_person_glue_dynamic_frame_mapping.toDF()
sales_salesorderheader_glue_df = sales_salesorderheader_glue_dynamic_frame_mapping.toDF()

'''
Creation of Temp view .
'''
sales_customer_df.createOrReplaceTempView('salesCustomerTable')
person_person_glue_df.createOrReplaceTempView(
    'personTable')
sales_salesorderheader_glue_df.createOrReplaceTempView('salesOrderTable')


third_query = '''
    SELECT FirstName , MiddleName , LastName , COUNT(1) as contagem_de_pedidos
    FROM((
        salesCustomerTable as sct
        INNER JOIN personTable as pt ON sct.PersonID = pt.BusinessEntityID)
        INNER JOIN salesOrderTable as sot ON sct.CustomerID = sot.CustomerID
    )
    GROUP BY FirstName , MiddleName , LastName
'''
third_query_result = spark.sql(third_query)

'''
Replace NULL values in MiddleName by empty space
'''
result_clean = third_query_result.fillna(
    " ", 'MiddleName').replace('NULL', '', 'MiddleName')

'''
Create a column with Full Name .
'''
result_clean_concat = result_clean.select(concat_ws(' ', result_clean.FirstName, result_clean.MiddleName, result_clean.LastName)
                                          .alias("FullName"), "contagem_de_pedidos")


# Convert back to dynamic frame
dynamic_frame_write = DynamicFrame.fromDF(
    result_clean_concat, glueContext, "dynamic_frame_write")

repartitioned_data_frame = dynamic_frame_write.repartition(1)

datasink2 = glueContext.write_dynamic_frame.from_options(frame=repartitioned_data_frame, connection_type="s3", connection_options={
                                                         "path": "s3://gluejobtasksoutput/write/Task-2/"}, format="json", transformation_ctx="datasink2")


job.commit()
