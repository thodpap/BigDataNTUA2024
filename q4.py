from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, row_number, broadcast
from pyspark.sql.types import BooleanType
import geopy.distance


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


def select_1xx(x_):
    x = str(x_)
    return x and len(x) == 3 and x[0] == "1"


def get_distance(x):
    key = x[0]
    coordinates_one, data_2 = x[1]
    coordinates_two, division = data_2

    return key, (geopy.distance.geodesic(coordinates_one, coordinates_two).km, 1, division)


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
                 csv_path_2="data/Crime_Data_from_2020_to_Present.csv",
                 lapd_stations_csv_path="data/LAPD_Stations_New.csv",
                 revgecoding_csv_path="data/revgecoding.csv"):
        self.spark = SparkSession.builder.appName(name).getOrCreate()
        self.crimes_csv_path = crimes_csv_path
        self.csv_path_2 = csv_path_2
        self.lapd_stations_csv_path = lapd_stations_csv_path
        self.revgecoding_csv_path = revgecoding_csv_path
        self.name = name

    def read_datasets(self):
        # Read the CSV file into a rdd
        rdd_ = self.spark.sparkContext.textFile(self.crimes_csv_path)
        header = rdd_.first()  # This is the header
        rdd_ = rdd_.filter(lambda line: line != header)

        rdd2 = self.spark.sparkContext.textFile(self.csv_path_2)
        header2 = rdd2.first()
        rdd2 = rdd2.filter(lambda line: line != header2)

        df = rdd_.map(parse_csv_line)
        df2 = rdd2.map(parse_csv_line)

        rdd = df.union(df2)

        # Filter only the 1xx on Weapon Used Cd
        rdd = rdd.filter(lambda x: select_1xx(x[16]))  # ["Weapon Used Cd"]

        # Filter with NULL Island
        df_crimes = rdd.filter(lambda x: x[-1] != "0" and x[-2] != "0")

        rdd_ = self.spark.sparkContext.textFile(self.lapd_stations_csv_path)
        header = rdd_.first()  # This is the header
        rdd_ = rdd_.filter(lambda line: line != header)
        df_lapd_stations = rdd_.map(parse_csv_line)

        return df_crimes, df_lapd_stations

    def query(self, join_type="broadcast"):
        crimes_rdd, lapd_stations_rdd = self.read_datasets()

        lapd_keyed_rdd = lapd_stations_rdd.map(lambda x: (int(x[-1]), ((float(x[1]), float(x[0])), x[3])))  # ["PREC"]
        crimes_keyed_rdd = crimes_rdd.map(lambda x: (int(x[4]), (float(x[-2]), float(x[-1]))))  # ["AREA "]

        if join_type == "broadcast":
            # Broadcast the departments RDD
            broadcast_departments = self.spark.sparkContext.broadcast(lapd_keyed_rdd.collectAsMap())
            print(broadcast_departments.value)

            joined_rdd = crimes_keyed_rdd.map(lambda x: (x[0], (x[1], broadcast_departments.value.get(x[0]))))
        elif join_type == "repartition":
            lapd_repartitioned = lapd_keyed_rdd.partitionBy(4)
            crimes_repartitioned = crimes_keyed_rdd.partitionBy(4)
            joined_rdd = crimes_repartitioned.join(lapd_repartitioned)
        else:
            joined_rdd = crimes_keyed_rdd.join(lapd_keyed_rdd)

        distance_rdd = joined_rdd.map(get_distance)

        final = (distance_rdd
                 .reduceByKey(lambda v, z: (v[0] + z[0], v[1] + z[1], v[2]))
                 .map(lambda x: (x[1][2],
                                 x[1][0] / x[1][1] if x[1][1] != 0.0 else 1000000,
                                 x[1][1]))
                 ).sortBy(lambda x: x[0])
        results = final.collect()[:10]
        for result in results:
            print(result)

    def clear_cache(self):
        self.spark.catalog.clearCache()
        self.spark.stop()
        self.spark = SparkSession.builder.appName(self.name).getOrCreate()
