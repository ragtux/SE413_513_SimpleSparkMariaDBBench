import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col, array_contains
import time

class SparkHelper:

    def __init__(self):
        self.spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
        self.df = self.spark.read.parquet(r"landsat_v2_metadata.parquet")


    def calculate_centroid(self):

        self.df.createOrReplaceTempView("PT")

        query = """
-- (1) calculate the center of scene using the midpoint formula
-- (2) calculate the average midpoint across all scenes (centroid of scenes)
SELECT 
    AVG(CALC_CENTER_LONG) AS AVG_CALC_CENTER_LONG,
    AVG(CALC_CENTER_LAT)  AS AVG_CALC_CENTER_LAT
FROM (
    SELECT
        ((LL_X + UR_X) / 2) AS CALC_CENTER_LONG,
        ((LL_Y + UR_Y) / 2) AS CALC_CENTER_LAT
    FROM (
        SELECT
            `CORNER LOWER LEFT LONGITUDE` AS LL_X,
            `CORNER LOWER LEFT LATITUDE` AS LL_Y,
            `CORNER UPPER RIGHT LATITUDE` AS UR_Y, 
            `CORNER UPPER RIGHT LONGITUDE` AS UR_X
        FROM PT
    ) A
) B"""


        sub_bench_start = time.time()
        res = self.spark.sql(query)
        sub_bench_end = time.time()

        delta = sub_bench_end - sub_bench_start
        return delta


