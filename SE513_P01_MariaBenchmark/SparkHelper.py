import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col, array_contains

class SparkHelper:


    def connect_spark(self):
        spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
        df = spark.read.csv(r"C:\Users\Bear\Downloads\landsat_ot_c2_l1_63edd1277577b0e2\L02_utf8.csv")

        df.printSchema()

        df2 = spark.read.option("header", True).csv(r"C:\Users\Bear\Downloads\landsat_ot_c2_l1_63edd1277577b0e2\L02_utf8.csv")

        # df2.printSchema()
        # df3 = spark.read.options(header='True', delimiter=',').csv("/apps/L02_utf8.csv")
        # df3.printSchema()

        df4 = df2.groupBy("collection category").count()
        df4.show()
