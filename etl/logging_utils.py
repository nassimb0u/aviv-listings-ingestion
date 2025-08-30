from pyspark.sql import SparkSession


def get_logger(spark: SparkSession):
    log4j = spark._jvm.org.apache.log4j
    return log4j.LogManager.getLogger(spark.sparkContext.appName)
