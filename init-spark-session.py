from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyProcess").master("local").getOrCreate()
