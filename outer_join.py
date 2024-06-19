from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("outer_join_coalesce").getOrCreate()

df1 = spark.createDataFrame([("a", 3), ("b", 4), ("c", 3)], ["name", "count_1"])
df2 = spark.createDataFrame([("a", 3), ("b", 4), ("d", 8)], ["name", "count_2"])

joined_df = (
    df1.join(df2, "name", "outer")
    .withColumn("name_clean", F.coalesce(df1["name"], df2["name"]))
    .drop("name")
)

## VS

joined_df_2 = df1.join(df2, "name", "outer")


joined_df.show()
joined_df_2.show()
