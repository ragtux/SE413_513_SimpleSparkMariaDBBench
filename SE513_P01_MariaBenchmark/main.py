import statistics
from MariaDBHelper import *
from SparkHelper import *
# from datetime import datetime, timedelta

if __name__ == '__main__':

    mdb_helper = MariaDBHelper()
    spark_helper = SparkHelper()

    bench_start = time.time()

    number_of_runs = 100

    maria_run_list = []
    spark_run_list = []
    for i in range(number_of_runs):
        maria_run_list.append(mdb_helper.calculate_centroid())
        spark_run_list.append(spark_helper.calculate_centroid())

    print("\n\n\n\n\n")
    print("- - - - SE513/413 GROUP 8 - CENTROID BENCH")
    print("NUMBER OF RUNS: " + str(number_of_runs))
    print(" MARIADB")
    print("        MEAN: " +  str(round(statistics.mean(maria_run_list), 3)) + " seconds")
    print("      STDDEV: " + str(round(statistics.stdev(maria_run_list), 3)) + " seconds")
    print(" PYSPARK")
    print("        MEAN: " +  str(round(statistics.mean(spark_run_list), 3)) + " seconds")
    print("      STDDEV: " + str(round(statistics.stdev(spark_run_list), 3)) + " seconds")


    maria_run_list = []
    spark_run_list = []
    for i in range(number_of_runs):
        maria_run_list.append(mdb_helper.calculate_cloud_coverage_over_80_percent())
        spark_run_list.append(spark_helper.calculate_cloud_coverage_over_80_percent())

    print("\n\n\n\n\n")
    print("- - - - SE513/413 GROUP 8 - CLOUD COVERAGE OVER 80% BENCH")
    print("NUMBER OF RUNS: " + str(number_of_runs))
    print(" MARIADB")
    print("        MEAN: " +  str(round(statistics.mean(maria_run_list), 3)) + " seconds")
    print("      STDDEV: " + str(round(statistics.stdev(maria_run_list), 3)) + " seconds")
    print(" PYSPARK")
    print("        MEAN: " +  str(round(statistics.mean(spark_run_list), 3)) + " seconds")
    print("      STDDEV: " + str(round(statistics.stdev(spark_run_list), 3)) + " seconds")


    maria_run_list = []
    spark_run_list = []
    for i in range(number_of_runs):
        maria_run_list.append(mdb_helper.calculate_satellite_distinct())
        spark_run_list.append(spark_helper.calculate_satellite_distinct())

    print("\n\n\n\n\n")
    print("- - - - SE513/413 GROUP 8 - COUNT OF SCENES FROM EACH SATELLITE")
    print("NUMBER OF RUNS: " + str(number_of_runs))
    print(" MARIADB")
    print("        MEAN: " +  str(round(statistics.mean(maria_run_list), 3)) + " seconds")
    print("      STDDEV: " + str(round(statistics.stdev(maria_run_list), 3)) + " seconds")
    print(" PYSPARK")
    print("        MEAN: " +  str(round(statistics.mean(spark_run_list), 3)) + " seconds")
    print("      STDDEV: " + str(round(statistics.stdev(spark_run_list), 3)) + " seconds")
