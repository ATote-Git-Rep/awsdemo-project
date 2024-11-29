import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_derived
from pyspark.sql.functions import month,year,to_date,col
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_path'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark.sql("show tables").show()

DDF = glueContext.create_dynamic_frame.from_catalog(database="atote-db", table_name="fy2025_yellow_tripdata_2015_01_csv", transformation_ctx="DDF")

NDF = DynamicFrame.toDF(DDF)
DF_New = NDF.withColumn('Year', year( to_date(col('tpep_pickup_datetime')))).withColumn('Month', month( to_date(col('tpep_pickup_datetime'))))

# covert normal df back to Dynamic
New_DDF = DynamicFrame.fromDF(DF_New, glueContext, "New_DDF")

# write DDF to s3 back
glueContext.write_dynamic_frame.from_options(\
frame = New_DDF,\
connection_options = {'path': args['s3_path'], "partitionKeys": ["Year", "Month", "passenger_count"]},\
connection_type = 's3',\
format = 'parquet')

job.commit()