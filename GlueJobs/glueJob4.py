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
# @args: [database = "gluedatabase", table_name = "sales_salesorderdetail_csv", transformation_ctx = "datasource0"]
# @return: sales_order_detail_glue_dynamic_frame
# @inputs: []
sales_order_detail_glue_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="gluedatabase", table_name="sales_salesorderdetail_csv", transformation_ctx="datasource0")

# @args: [database = "gluedatabase", table_name = "sales_salesorderheader_csv", transformation_ctx = "datasource0"]
# @return: sales_salesorderheader_glue_dynamic_frame
# @inputs: []
sales_salesorderheader_glue_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="gluedatabase", table_name="sales_salesorderheader_csv", transformation_ctx="datasource0")

# @args: [database = "gluedatabase", table_name = "production_product_csv", transformation_ctx = "datasource0"]
# @return: production_product_csv_glue_dynamic_frame
# @inputs: []
production_product_csv_glue_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="gluedatabase", table_name="production_product_csv", transformation_ctx="datasource0")


'''
Mapping data to glueContext . 
'''
# @type: ApplyMapping
# @args: [mapping = [("salesorderid", "long", "salesorderid", "long"), ("salesorderdetailid", "long", "salesorderdetailid", "long"), ("carriertrackingnumber", "string", "carriertrackingnumber", "string"), ("orderqty", "long", "orderqty", "long"), ("productid", "long", "productid", "long"), ("specialofferid", "long", "specialofferid", "long"), ("unitprice", "string", "unitprice", "string"), ("unitpricediscount", "string", "unitpricediscount", "string"), ("linetotal", "double", "linetotal", "double"), ("rowguid", "string", "rowguid", "string"), ("modifieddate", "string", "modifieddate", "string")], transformation_ctx = "applymapping1"]
# @return: sales_order_detail_glue_dynamic_frame_mapping
# @inputs: [frame = datasource0]
sales_order_detail_glue_dynamic_frame_mapping = ApplyMapping.apply(frame=sales_order_detail_glue_dynamic_frame, mappings=[("salesorderid", "long", "salesorderid", "long"), ("salesorderdetailid", "long", "salesorderdetailid", "long"), ("carriertrackingnumber", "string", "carriertrackingnumber", "string"), ("orderqty", "long", "orderqty", "long"), (
    "productid", "long", "productid", "long"), ("specialofferid", "long", "specialofferid", "long"), ("unitprice", "string", "unitprice", "string"), ("unitpricediscount", "string", "unitpricediscount", "string"), ("linetotal", "double", "linetotal", "double"), ("rowguid", "string", "rowguid", "string"), ("modifieddate", "string", "modifieddate", "string")], transformation_ctx="applymapping1")

# @type: ApplyMapping
# @args: [mapping = [("salesorderid", "long", "salesorderid", "long"), ("revisionnumber", "long", "revisionnumber", "long"), ("orderdate", "string", "orderdate", "string"), ("duedate", "string", "duedate", "string"), ("shipdate", "string", "shipdate", "string"), ("status", "long", "status", "long"), ("onlineorderflag", "long", "onlineorderflag", "long"), ("salesordernumber", "string", "salesordernumber", "string"), ("purchaseordernumber", "string", "purchaseordernumber", "string"), ("accountnumber", "string", "accountnumber", "string"), ("customerid", "long", "customerid", "long"), ("salespersonid", "long", "salespersonid", "long"), ("territoryid", "long", "territoryid", "long"), ("billtoaddressid", "long", "billtoaddressid", "long"), ("shiptoaddressid", "long", "shiptoaddressid", "long"), ("shipmethodid", "long", "shipmethodid", "long"), ("creditcardid", "long", "creditcardid", "long"), ("creditcardapprovalcode", "string", "creditcardapprovalcode", "string"), ("currencyrateid", "long", "currencyrateid", "long"), ("subtotal", "string", "subtotal", "string"), ("taxamt", "string", "taxamt", "string"), ("freight", "string", "freight", "string"), ("totaldue", "string", "totaldue", "string"), ("comment", "string", "comment", "string"), ("rowguid", "string", "rowguid", "string"), ("modifieddate", "string", "modifieddate", "string")], transformation_ctx = "applymapping1"]
# @return: applymapping1
# @inputs: [frame = datasource0]
sales_salesorderheader_glue_dynamic_frame_mapping = ApplyMapping.apply(frame=sales_salesorderheader_glue_dynamic_frame, mappings=[("salesorderid", "long", "salesorderid", "long"), ("revisionnumber", "long", "revisionnumber", "long"), ("orderdate", "string", "orderdate", "string"), ("duedate", "string", "duedate", "string"), ("shipdate", "string", "shipdate", "string"), ("status", "long", "status", "long"), ("onlineorderflag", "long", "onlineorderflag", "long"), ("salesordernumber", "string", "salesordernumber", "string"), ("purchaseordernumber", "string", "purchaseordernumber", "string"), ("accountnumber", "string", "accountnumber", "string"), ("customerid", "long", "customerid", "long"), ("salespersonid", "long", "salespersonid", "long"), (
    "territoryid", "long", "territoryid", "long"), ("billtoaddressid", "long", "billtoaddressid", "long"), ("shiptoaddressid", "long", "shiptoaddressid", "long"), ("shipmethodid", "long", "shipmethodid", "long"), ("creditcardid", "long", "creditcardid", "long"), ("creditcardapprovalcode", "string", "creditcardapprovalcode", "string"), ("currencyrateid", "long", "currencyrateid", "long"), ("subtotal", "string", "subtotal", "string"), ("taxamt", "string", "taxamt", "string"), ("freight", "string", "freight", "string"), ("totaldue", "string", "totaldue", "string"), ("comment", "string", "comment", "string"), ("rowguid", "string", "rowguid", "string"), ("modifieddate", "string", "modifieddate", "string")], transformation_ctx="applymapping1")

# @type: ApplyMapping
# @args: [mapping = [("productid", "long", "productid", "long"), ("name", "string", "name", "string"), ("productnumber", "string", "productnumber", "string"), ("makeflag", "long", "makeflag", "long"), ("finishedgoodsflag", "long", "finishedgoodsflag", "long"), ("color", "string", "color", "string"), ("safetystocklevel", "long", "safetystocklevel", "long"), ("reorderpoint", "long", "reorderpoint", "long"), ("standardcost", "string", "standardcost", "string"), ("listprice", "string", "listprice", "string"), ("size", "string", "size", "string"), ("sizeunitmeasurecode", "string", "sizeunitmeasurecode", "string"), ("weightunitmeasurecode", "string", "weightunitmeasurecode", "string"), ("weight", "double", "weight", "double"), ("daystomanufacture", "long", "daystomanufacture", "long"), ("productline", "string", "productline", "string"), ("class", "string", "class", "string"), ("style", "string", "style", "string"), ("productsubcategoryid", "long", "productsubcategoryid", "long"), ("productmodelid", "long", "productmodelid", "long"), ("sellstartdate", "string", "sellstartdate", "string"), ("sellenddate", "string", "sellenddate", "string"), ("discontinueddate", "string", "discontinueddate", "string"), ("rowguid", "string", "rowguid", "string"), ("modifieddate", "string", "modifieddate", "string")], transformation_ctx = "applymapping1"]
# @return: production_product_csv_glue_dynamic_frame_mapping
# @inputs: [frame = datasource0]
production_product_csv_glue_dynamic_frame_mapping = ApplyMapping.apply(frame=production_product_csv_glue_dynamic_frame, mappings=[("productid", "long", "productid", "long"), ("name", "string", "name", "string"), ("productnumber", "string", "productnumber", "string"), ("makeflag", "long", "makeflag", "long"), ("finishedgoodsflag", "long", "finishedgoodsflag", "long"), ("color", "string", "color", "string"), ("safetystocklevel", "long", "safetystocklevel", "long"), ("reorderpoint", "long", "reorderpoint", "long"), ("standardcost", "string", "standardcost", "string"), ("listprice", "string", "listprice", "string"), ("size", "string", "size", "string"), ("sizeunitmeasurecode", "string", "sizeunitmeasurecode", "string"), (
    "weightunitmeasurecode", "string", "weightunitmeasurecode", "string"), ("weight", "double", "weight", "double"), ("daystomanufacture", "long", "daystomanufacture", "long"), ("productline", "string", "productline", "string"), ("class", "string", "class", "string"), ("style", "string", "style", "string"), ("productsubcategoryid", "long", "productsubcategoryid", "long"), ("productmodelid", "long", "productmodelid", "long"), ("sellstartdate", "string", "sellstartdate", "string"), ("sellenddate", "string", "sellenddate", "string"), ("discontinueddate", "string", "discontinueddate", "string"), ("rowguid", "string", "rowguid", "string"), ("modifieddate", "string", "modifieddate", "string")], transformation_ctx="applymapping1")


'''
Convert dynamic frame to data frame . 
'''
sales_order_detail_glue_df = sales_order_detail_glue_dynamic_frame.toDF()
sales_salesorderheader_glue_df = sales_salesorderheader_glue_dynamic_frame_mapping.toDF()
production_product_csv_glue_df = production_product_csv_glue_dynamic_frame_mapping.toDF()

'''
Creation of Temp view .
'''
sales_order_detail_glue_df.createOrReplaceTempView('salesOrderDetailTable')
sales_salesorderheader_glue_df.createOrReplaceTempView('salesOrderTable')
production_product_csv_glue_df.createOrReplaceTempView('productTable')

'''
Query to fetch the total quantity of products by ProductID and OrderDate .
'''

fourth_query = '''
    SELECT SUM(OrderQty) as somaTotal
    FROM (( salesOrderDetailTable as sodt
        INNER JOIN salesOrderTable as sot ON sodt.SalesOrderID = sot.SalesOrderID)
        INNER JOIN productTable as pt ON sodt.ProductID = pt.ProductID
    )
    GROUP BY pt.ProductID , OrderDate
'''

fourth_query_result = spark.sql(fourth_query)

# Convert back to dynamic frame
dynamic_frame_write = DynamicFrame.fromDF(
    fourth_query_result, glueContext, "dynamic_frame_write")

repartitioned_data_frame = dynamic_frame_write.repartition(1)

datasink2 = glueContext.write_dynamic_frame.from_options(frame=repartitioned_data_frame, connection_type="s3", connection_options={
                                                         "path": "s3://gluejobtasksoutput/write/Task-4/"}, format="json", transformation_ctx="datasink2")


job.commit()
