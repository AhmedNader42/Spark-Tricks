import pyspark
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("parquet_filter_date").getOrCreate()

data = [
    ("A ", "202201"),
    ("B ", "202209"),
    ("C ", "202210"),
    ("D ", "202301"),
]

columns = ["name", "dob"]
df = spark.createDataFrame(data, columns)

df.write.partitionBy("dob").mode("overwrite").parquet("people.parquet")

people = spark.read.parquet("people.parquet")
people_filtered_without_function = people.filter("dob >= 202201")
people_filtered_with_function = people.filter("substring(dob, 1, 4) >= 2022")

people_filtered_without_function.explain()
people_filtered_with_function.explain()
