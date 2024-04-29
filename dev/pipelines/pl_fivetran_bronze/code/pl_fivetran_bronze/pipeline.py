from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pl_fivetran_bronze.config.ConfigStore import *
from pl_fivetran_bronze.udfs.UDFs import *
from prophecy.utils import *
from pl_fivetran_bronze.graph import *

def pipeline(spark: SparkSession) -> None:
    df_ds_ingest_forecast = ds_ingest_forecast(spark)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("pl_fivetran_bronze")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/pl_fivetran_bronze")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/pl_fivetran_bronze", config = Config)(pipeline)

if __name__ == "__main__":
    main()
