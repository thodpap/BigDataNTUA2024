from pyspark.sql import SparkSession
from geopy.distance import geodesic
from pyspark.sql.functions import col, udf, avg, count, row_number, broadcast
from pyspark.sql.types import DoubleType

hdfs_path = 'hdfs://master:9000/datasets/'



def print_dictionary(dictionary):
    # Extract the data and organize it into rows
    rows = []
    for file_type, methods in dictionary.items():
        for method, elapsed_time in methods.items():
            rows.append([file_type, method, elapsed_time])

    # Define the header
    header = ["file_type", "method", "elapsed_time"]

    # Define the column widths
    col_widths = [max(len(str(item)) for item in col) for col in zip(*([header] + rows))]

    # Create a format string for each row
    row_format = '| ' + ' | '.join(f'{{:<{width}}}' for width in col_widths) + ' |'

    # Print the header
    print(row_format.format(*header))
    print('|-' + '-|-'.join('-' * width for width in col_widths) + '-|')

    # Print each row of the table
    for row in rows:
        print(row_format.format(*row))
    print('|-' + '-|-'.join('-' * width for width in col_widths) + '-|')


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


def get_distance_rdd(x):
    key = x[0]
    coordinates_one, data_2 = x[1]
    coordinates_two, division = data_2

    return key, (geodesic(coordinates_one, coordinates_two).km, 1, division)


def get_distance(coordinates_one, coordinates_two):
    return geodesic(coordinates_one, coordinates_two).km


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
                 # crimes_csv_path=hdfs_path + "Crime_Data_from_2010_to_2019.csv",
                 # csv_path_2=hdfs_path + "Crime_Data_from_2020_to_Present.csv",
                 # lapd_stations_csv_path=hdfs_path + "LAPD_Stations_New.csv",
                 revgecoding_csv_path="revgecoding.csv"):
        self.spark = SparkSession.builder.appName(name).getOrCreate()

        self.crimes_csv_path = crimes_csv_path
        self.csv_path_2 = csv_path_2
        self.lapd_stations_csv_path = lapd_stations_csv_path
        self.revgecoding_csv_path = revgecoding_csv_path
        self.name = name

    def read_datasets_csv(self):
        df = self.spark.read.csv(self.crimes_csv_path, header=True, inferSchema=True)
        df2 = self.spark.read.csv(self.csv_path_2, header=True, inferSchema=True)
        df = df.union(df2)

        select_1xx_udf = udf(lambda x: select_1xx(x))
        df = df.withColumn("weapon_boolean", select_1xx_udf(col("Weapon Used Cd"))).where(col("weapon_boolean") == True)
        df_crimes = df.where((col("LAT") != 0.0) & (col("LON") != 0.0))

        df_lapd_stations = self.spark.read.csv(self.lapd_stations_csv_path, header=True, inferSchema=True)

        return df_crimes, df_lapd_stations

    def read_datasets_rdd(self):
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

    def rdd_query(self, join_type):
        crimes_rdd, lapd_stations_rdd = self.read_datasets_rdd()

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

        distance_rdd = joined_rdd.map(get_distance_rdd)

        final = (distance_rdd
                 .reduceByKey(lambda v, z: (v[0] + z[0], v[1] + z[1], v[2]))
                 .map(lambda x: (x[1][2],
                                 x[1][0] / x[1][1] if x[1][1] != 0.0 else 1000000,
                                 x[1][1]))
                 ).sortBy(lambda x: x[2], ascending=False)
        results = final.collect()[:10]
        for result in results:
            print(result)

    def spark_query(self, join_type=""):
        df_crimes, df_lapd_stations = self.read_datasets_csv()

        # Select necessary columns
        df_crimes = df_crimes.select(col("AREA "), col("LAT"), col("LON"))
        # df_lapd_stations = df_lapd_stations.select(col("PREC"), col("Y"), col("X"), col("STATION"))

        # Perform join
        if join_type == "broadcast":
            broadcast_lapd_stations = self.spark.sparkContext.broadcast(df_lapd_stations.collect())
            broadcast_df = self.spark.createDataFrame(broadcast_lapd_stations.value)
            joined_df = df_crimes.join(broadcast_df, df_crimes["AREA "] == broadcast_df["PREC"], how="left")
        elif join_type == "repartition":
            df_crimes = df_crimes.repartition(4, col("AREA "))
            df_lapd_stations = df_lapd_stations.repartition(4, col("PREC"))
            joined_df = df_crimes.join(df_lapd_stations, df_crimes["AREA "] == df_lapd_stations["PREC"], how="inner")
        else:
            joined_df = df_crimes.join(df_lapd_stations, df_crimes["AREA "] == df_lapd_stations["PREC"], how="inner")

        get_distance_udf = udf(lambda lat1, lon1, lat2, lon2: get_distance((lat1, lon1), (lat2, lon2)), DoubleType())

        # Calculate distances
        joined_df = joined_df.withColumn("distance", get_distance_udf(col("LAT"), col("LON"), col("Y"),
                                                                      col("X")))

        # Aggregate results
        final_df = joined_df.groupBy("DIVISION").agg(
            avg("distance").alias("avg_distance"),
            count("*").alias("count")
        ).sort(col("count").desc())

        final_df.show()

    def query(self, join_type="broadcast", type_="rdd"):
        if type_ == "rdd":
            self.rdd_query(join_type)
        elif type_ == "spark":
            self.spark_query(join_type)
        else:
            raise ValueError("Not implemented")

    def clear_cache(self):
        self.spark.catalog.clearCache()
        self.spark.stop()
        self.spark = SparkSession.builder.appName(self.name).getOrCreate()


if __name__ == "__main__":
    import time

    Q4 = Q4("Q4")

    dictionary = {}
    for join_type in ["broadcast", "repartition", "none"]:
        if join_type not in dictionary:
            dictionary[join_type] = {}
        for type_ in ["rdd", "spark"]:
            start_time = time.time()
            Q4.query(join_type, type_)
            elapsed_time = time.time() - start_time
            print(f"Elapsed Time for csv rdd with {type_}: {elapsed_time}")
            Q4.clear_cache()
            dictionary[join_type][type_] = elapsed_time

    print_dictionary(dictionary)
