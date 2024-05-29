from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, row_number
from pyspark.sql.types import BooleanType
import geopy.distance


def select_1xx(x_):
    x = str(x_)
    return x and len(x) == 3 and x[0] == "1"


def get_distance(x):
    key = x[0]
    coordinates_one, coordinates_two = x[1]
    return key, (geopy.distance.geodesic(coordinates_one, coordinates_two).km, 1)


def format_coordinates(x, pos_lat=1, pos_lon=0):
    lat, lon = str(x[pos_lat]), str(x[pos_lon])

    lat_parts = lat.split('.')
    lon_parts = lon.split('.')

    # Format the latitude and longitude to 4 decimal places
    formatted_lat = lat_parts[0] + "." + lat_parts[1][:4]
    formatted_lon = lon_parts[0] + "." + lon_parts[1][:4]

    # Ensure the string has a length of 4 after the decimal point (padding if necessary)
    if len(formatted_lat.split('.')[1]) < 4:
        formatted_lat += '0' * (4 - len(formatted_lat.split('.')[1]))
    if len(formatted_lon.split('.')[1]) < 4:
        formatted_lon += '0' * (4 - len(formatted_lon.split('.')[1]))

    # Return the formatted string
    return f"{formatted_lat},{formatted_lon}", x


class Q4:
    def __init__(self, name,
                 crimes_csv_path="data/Crime_Data_from_2010_to_2019.csv",
                 lapd_stations_csv_path="data/LAPD_Stations_New.csv",
                 revgecoding_csv_path="data/revgecoding.csv"):
        self.spark = SparkSession.builder.appName(name).getOrCreate()
        self.crimes_csv_path = crimes_csv_path
        self.lapd_stations_csv_path = lapd_stations_csv_path
        self.revgecoding_csv_path = revgecoding_csv_path
        self.name = name

    def read_datasets(self):
        df_crimes = self.spark.read.csv(self.crimes_csv_path, header=True, inferSchema=True)
        # df_crimes.printSchema()

        # Filter only the 1xx on Weapon Used Cd
        select_1xx_udf = udf(lambda x: select_1xx(x), BooleanType())
        df_crimes = df_crimes.filter(select_1xx_udf(df_crimes["Weapon Used Cd"]))

        # Filter with NULL Island
        df_crimes = df_crimes.where((col("LON") != 0.0) & (col("LAT") != 0.0))

        df_lapd_stations = self.spark.read.csv(self.lapd_stations_csv_path, header=True, inferSchema=True)

        return df_crimes.rdd, df_lapd_stations.rdd

    def query(self):
        crimes_rdd, lapd_stations_rdd = self.read_datasets()

        # Might be needed
        # lapd_keyed_rdd = lapd_stations_rdd.map(format_coordinates)

        lapd_keyed_rdd = lapd_stations_rdd.map(lambda x: (x["PREC"], (x[1], x[0])))
        crimes_keyed_rdd = crimes_rdd.map(lambda x: (x["AREA "], (x[-2], x[-1])))

        joined_data_rdd = crimes_keyed_rdd.join(lapd_keyed_rdd)

        distance_rdd = joined_data_rdd.map(get_distance)

        final = (distance_rdd
                 .reduceByKey(lambda v, z: (v[0] + z[0], v[1] + z[1]))
                 .map(lambda x: (x[0],
                                 x[1][0] / x[1][1] if x[1][1] != 0.0 else 1000000,
                                 x[1][1]))
                 )
        results = final.collect()[:10]
        for result in results:
            print(result)

    def clear_cache(self):
        self.spark.catalog.clearCache()
        self.spark.stop()
        self.spark = SparkSession.builder.appName(self.name).getOrCreate()
