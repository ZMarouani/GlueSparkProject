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

# @args: [database = "gluedatabase", table_name = "sales_specialofferproduct_csv", transformation_ctx = "datasource0"]
# @return: sales_specialofferproduct_glue_dynamic_frame
# @inputs: []
sales_specialofferproduct_glue_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="gluedatabase", table_name="sales_specialofferproduct_csv", transformation_ctx="datasource0")

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
# @args: [mapping = [("specialofferid", "long", "specialofferid", "long"), ("productid", "long", "productid", "long"), ("rowguid", "string", "rowguid", "string"), ("modifieddate", "string", "modifieddate", "string")], transformation_ctx = "applymapping1"]
# @return: sales_specialofferproduct_glue_dynamic_frame_mapping
# @inputs: [frame = datasource0]
sales_specialofferproduct_glue_dynamic_frame_mapping = ApplyMapping.apply(frame=sales_specialofferproduct_glue_dynamic_frame, mappings=[("specialofferid", "long", "specialofferid", "long"), (
    "productid", "long", "productid", "long"), ("rowguid", "string", "rowguid", "string"), ("modifieddate", "string", "modifieddate", "string")], transformation_ctx="applymapping1")

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
sales_specialofferproduct_glue_df = sales_specialofferproduct_glue_dynamic_frame_mapping.toDF()
production_product_csv_glue_df = production_product_csv_glue_dynamic_frame_mapping.toDF()

'''
Creation of Temp view .
'''
sales_order_detail_glue_df.createOrReplaceTempView('salesOrderDetailTable')
sales_specialofferproduct_glue_df.createOrReplaceTempView(
    'specialOfferProductTable')
production_product_csv_glue_df.createOrReplaceTempView('productTable')


'''
Query that fetches most 3 sold products per days to Manufacture.
'''
second_query = '''
    SELECT Name 
    FROM (( specialOfferProductTable as sopt
    INNER JOIN salesOrderDetailTable as sodt ON sopt.SpecialOfferID = sodt.SpecialOfferID )
    INNER JOIN productTable as pt ON sopt.ProductID = pt.productID
    )
    GROUP BY DaysToManufacture , Name
    ORDER BY SUM(OrderQty) DESC
    LIMIT 3 
'''


second_query_result = spark.sql(second_query)

# Convert back to dynamic frame
dynamic_frame_write = DynamicFrame.fromDF(
    second_query_result, glueContext, "dynamic_frame_write")

repartitioned_data_frame = dynamic_frame_write.repartition(1)

datasink2 = glueContext.write_dynamic_frame.from_options(frame=repartitioned_data_frame, connection_type="s3", connection_options={
                                                         "path": "s3://gluejobtasksoutput/write/Task-2/"}, format="json", transformation_ctx="datasink2")


job.commit()
