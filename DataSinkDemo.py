from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id
from lib.logger import Log4J


if __name__ == '__main__':
    spark = SparkSession.builder \
            .master("local[3]") \
            .appName("DataSinkDemo") \
            .getOrCreate()
    
    flight_time_parquet = spark.read \
                        .format("parquet") \
                        .load("./dataSource/flight-time.parquet")
    
    logger = Log4J(spark)
    flight_time_parquet.show(10)

    logger.info(f"Number of partitions: {flight_time_parquet.rdd.getNumPartitions()}")
    flight_time_parquet.groupBy(spark_partition_id().alias("partition id")).count().show()

    flight_time_parquet.write \
        .format("avro") \
        .mode("overwrite") \
        .option("path", "./dataSink/avro/") \
        .save()
    
    