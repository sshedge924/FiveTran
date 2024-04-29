from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from pl_fivetran_bronze.config.ConfigStore import *
from pl_fivetran_bronze.udfs.UDFs import *

def ds_ingest_forecast(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("CustomerID", StringType(), True), StructField("FirstName", StringType(), True), StructField("LastName", StringType(), True), StructField("City", StringType(), True), StructField("DateCreated", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/mnt/ipcontainer/soda_scd2/scd2_file1.csv")
