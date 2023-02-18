import sys
import mariadb
import time

class MariaDBHelper:

    def __init__(self):
        # Connect to MariaDB Platform
        try:
            self.conn = mariadb.connect(
                user="root",
                password="bear",
                host="127.0.0.1",
                port=3306,
            )

        except mariadb.Error as e:
            print(f"ERROR CONNECTING TO MARIADB: {e}")
            sys.exit(1)

    def calculate_centroid(self):

        query = """
-- (1) calculate the center of scene using the midpoint formula
-- (2) calculate the average midpoint across all scenes (centroid of scenes)
SELECT 
	AVG(CALC_CENTER_LONG),
	AVG(CALC_CENTER_LAT)
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
		FROM SE413_513.L02_UTF8
	) A
) B"""

        cur = self.conn.cursor()

        sub_bench_start = time.time()
        cur.execute(query)
        sub_bench_end = time.time()

        delta = sub_bench_end - sub_bench_start

        return delta

    def
