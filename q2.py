from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

def find_part_of_day(time_occ_):
    time_occ = str(time_occ_)
    if len(time_occ) < 4:
        '''0020 =>(int) 20 => "00" + "20" => "0020"'''
        a = 4 - len(time_occ)
        s = "0" * a
        time_occ = s + time_occ

    if "2100" <= time_occ or time_occ < "0500":
        return "NIGHT"

    if "0500" <= time_occ < "1200":
        return "MORNING"

    if "1200" <= time_occ < "1700":
        return "MIDDAY"

    if "1700" <= time_occ < "2100":
        return "EVENING"

    raise ValueError(f"Something bad {time_occ}")


def parse_csv_line(line):
    result = []
    current = ''
    in_quotes = False
    for char in line:
        if char == '"' and (not current or current[-1] != '\\'):  # Check for quote that is not escaped
            in_quotes = not in_quotes  # Toggle state
        elif char == ',' and not in_quotes:
            result.append(current)
            current = ''
        else:
            current += char
    if current:
        result.append(current)
    return result

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
        .filter(lambda row: row[3] == "STREET")
        .map(lambda row: (row[3], row[15]))
        .map(lambda row: (find_part_of_day(row[0]), 1))
        .reduceByKey(lambda a, b: a+b)
        .map(lambda row: (row[1], row[0]))
        .sortByKey(ascending=False)
    )
    for item in parsed_data.collect():
        print(item)


class Q2:
    def __init__(self, name, csv_path="data/Crime_Data_from_2010_to_2019.csv",
                 csv_path_2="data/Crime_Data_from_2020_to_Present.csv"):
        self.spark = SparkSession.builder.appName(name).getOrCreate()
        self.csv_path = csv_path
        self.csv_path_2 = csv_path_2
        self.name = name

    def query(self, file_type="csv", method="spark_sql", run_filter_first=True):
        if method == "rdd":
            # df = self.spark.read.csv(self.csv_path, header=True, inferSchema=True).rdd
            rdd_ = self.spark.sparkContext.textFile(self.csv_path)
            rdd2 = self.spark.sparkContext.textFile(self.csv_path_2)

            header = rdd_.first()  # This is the header
            header2 = rdd2.first()

            rdd_ = rdd_.filter(lambda line: line != header)
            rdd2 = rdd2.filter(lambda line: line != header2)

            df = rdd_.map(parse_csv_line)
            df2 = rdd2.map(parse_csv_line)

            df = df.union(df2)
        elif file_type == "csv":
            df = self.spark.read.csv(self.csv_path, header=True, inferSchema=True)
            df2 = self.spark.read.csv(self.csv_path_2, header=True, inferSchema=True)
            df = df.union(df2)
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
        self.spark.stop()
        self.spark = SparkSession.builder.appName(self.name).getOrCreate()
