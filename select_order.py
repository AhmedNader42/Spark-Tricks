from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("parquet_filter_date").getOrCreate()

data = [("Alarms",), ("Alarms",), ("Traffic Collision",), ("Other",)]

columns = ["calltype"]
df = spark.createDataFrame(data, columns)

fire_df_convrtd_grpby = (
    df.select("CallType")
    .where("CallType is not null")
    .groupBy("CallType")
    .count()
    .orderBy("count", ascending=False)
    .show()
)

fire_df_convrtd_grpby = (
    df.where("CallType is not null")
    .groupBy("CallType")
    .count()
    .select("CallType", "count")
    .orderBy("count", ascending=False)
    .show()
)
