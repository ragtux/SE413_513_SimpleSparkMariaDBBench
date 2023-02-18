import SparkHelper
from MariaDBHelper import *
import pandas as pd
# from datetime import datetime, timedelta

if __name__ == '__main__':

    mdb_helper = MariaDBHelper()

    bench_start = time.time()

    run_list = []
    for i in range(10):
        run_list.append(mdb_helper.calculate_centroid())
    s1 = pd.Series(run_list)
