from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, row_number
from pyspark.sql.types import BooleanType


def select_1xx(x_):
    x = str(x_)
    return x and len(x) == 3 and x[0] == "1"


class Q4:
    def __init__(self, name,
                 crimes_csv_path="data/Crime_Data_from_2010_to_2019.csv",
                 lapd_stations_csv_path="data/LAPD_Stations.csv"):
        self.spark = SparkSession.builder.appName(name).getOrCreate()
        self.crimes_csv_path = crimes_csv_path
        self.lapd_stations_csv_path = lapd_stations_csv_path

    def read_datasets(self):
        df_crimes = self.spark.read.csv(self.crimes_csv_path, header=True, inferSchema=True)

        # Filter only the 1xx on Weapon Used Cd
        select_1xx_udf = udf(lambda x: select_1xx(x), BooleanType())
        df_crimes = df_crimes.filter(select_1xx_udf(df_crimes["Weapon Used Cd"]))

        # Filter with NULL Island
        df_crimes = df_crimes.where(col("LOCATION") != "ISLAND")

        df_lapd_stations = self.spark.read.csv(self.lapd_stations_csv_path, header=True, inferSchema=True)

        return df_crimes.rdd, df_lapd_stations.rdd

    def query(self):
        crimes_rdd, lapd_stations_rdd = self.read_datasets()

        # Join crimes with lapd