
from pyspark.sql.functions import col, dayofmonth, year, month
df = spark.read.json("s3://{{ var.value['daily_spark_job.aws_bucket'] }}/{{ ds|replace("-", "/") }}/*") \
    .withColumn("date", (col("event_time").cast("date"))) \
    .withColumn("year", year('date')) \
    .withColumn("month", month('date')) \
    .withColumn("day", dayofmonth('date'))
df.write.partitionBy(['year', 'month', 'day']).format("parquet").save("s3://{{ var.value['daily_spark_job.aws_bucket'] }}/parquet/")
