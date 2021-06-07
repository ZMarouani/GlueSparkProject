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

'''
Mapping data to glueContext . 
'''
# @type: ApplyMapping
# @args: [mapping = [("salesorderid", "long", "salesorderid", "long"), ("salesorderdetailid", "long", "salesorderdetailid", "long"), ("carriertrackingnumber", "string", "carriertrackingnumber", "string"), ("orderqty", "long", "orderqty", "long"), ("productid", "long", "productid", "long"), ("specialofferid", "long", "specialofferid", "long"), ("unitprice", "string", "unitprice", "string"), ("unitpricediscount", "string", "unitpricediscount", "string"), ("linetotal", "double", "linetotal", "double"), ("rowguid", "string", "rowguid", "string"), ("modifieddate", "string", "modifieddate", "string")], transformation_ctx = "applymapping1"]
# @return: applymapping1
# @inputs: [frame = datasource0]
sales_order_detail_glue_dynamic_frame_mapping = ApplyMapping.apply(frame=sales_order_detail_glue_dynamic_frame, mappings=[("salesorderid", "long", "salesorderid", "long"), ("salesorderdetailid", "long", "salesorderdetailid", "long"), ("carriertrackingnumber", "string", "carriertrackingnumber", "string"), ("orderqty", "long", "orderqty", "long"), (
    "productid", "long", "productid", "long"), ("specialofferid", "long", "specialofferid", "long"), ("unitprice", "string", "unitprice", "string"), ("unitpricediscount", "string", "unitpricediscount", "string"), ("linetotal", "double", "linetotal", "double"), ("rowguid", "string", "rowguid", "string"), ("modifieddate", "string", "modifieddate", "string")], transformation_ctx="applymapping1")

'''
Convert dynamic frame to data frame . 
'''
sales_order_detail_glue_df = sales_order_detail_glue_dynamic_frame.toDF()


'''
Creation of Temp view .
'''
sales_order_detail_glue_df.createOrReplaceTempView('salesOrderDetailTable')


'''
Query that returns number of products having at least 3 Details .
'''
first_query = '''SELECT COUNT(1) as Details , SalesOrderID 
    FROM salesOrderDetailTable 
    GROUP BY SalesOrderID
    HAVING Details > 2 '''


first_query_result = spark.sql(first_query)

# Convert back to dynamic frame
dynamic_frame_write = DynamicFrame.fromDF(
    first_query_result, glueContext, "dynamic_frame_write")

repartitioned_data_frame = dynamic_frame_write.repartition(1)

datasink2 = glueContext.write_dynamic_frame.from_options(frame=repartitioned_data_frame, connection_type="s3", connection_options={
                                                         "path": "s3://gluejobtasksoutput/write"}, format="json", transformation_ctx="datasink2")


job.commit()
