from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType


def find_part_of_day(time_occ_):
    time_occ = str(time_occ_)
    if "2100" <= time_occ or time_occ < "0500":
        return "NIGHT"

    if "0500" <= time_occ < "1200":
        return "MORNING"

    if "1200" <= time_occ < "1700":
        return "MIDDAY"

    if "1700" <= time_occ < "2100":
        return "EVENING"

    raise ValueError(f"Something bad {time_occ}")


def spark_sql(df, run_filter_first=True):
    if run_filter_first:
        df = df.where(col("Premis Desc") == "STREET")

    part_of_day_udf = udf(lambda x: find_part_of_day(x))
    df = df.withColumn("day_part", part_of_day_udf(col("TIME OCC")))

    if not run_filter_first:
        df = df.where(col("Premis Desc") == "STREET")

    df = df.groupBy("day_part").count().orderBy(col("count").desc())

    df.select("day_part", "COUNT").show()


def rdd(df_rdd):
    parsed_data = (
        df_rdd
        .map(lambda row: (row[3], row[15]))
        .filter(lambda row: row[1] == "STREET")
        .map(lambda row: (find_part_of_day(row[0]), 1))
        .reduceByKey(lambda a, b: a+b)
        .map(lambda row: (row[1], row[0]))
        .sortByKey(ascending=False)
    )
    for item in parsed_data.collect():
        print(item)


class Q2:
    def __init__(self, name, csv_path="data/Crime_Data_from_2010_to_2019.csv"):
        self.spark = SparkSession.builder.appName(name).getOrCreate()
        self.csv_path = csv_path

    def query(self, file_type="csv", method="spark_sql", run_filter_first=True):
        if method == "rdd":
            # reading through the text file might be faster, however, in columns where it has
            # text as "some text, some other text", when we split with "," we don't actually split hte commas but rather
            # we split any comma, meaning we don't get the proper scheme of our data.
            # rdd_ = self.spark.sparkContext.textFile(self.csv_path)
            # header = rdd_.first()  # This is the header
            # rdd_ = rdd_.filter(lambda line: line != header)

            df = self.spark.read.csv(self.csv_path, header=True, inferSchema=True).rdd
        elif file_type == "csv":
            df = self.spark.read.csv(self.csv_path, header=True, inferSchema=True)
        else:
            raise ValueError("Wrong file type")

        if method == "spark_sql":
            spark_sql(df, run_filter_first=run_filter_first)
        elif method == "rdd":
            rdd(df)
        else:
            raise ValueError("Wrong value")

    def clear_cache(self):
        self.spark.catalog.clearCache()
