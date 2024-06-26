from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, rank

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


def spark_sql(df):
    # TODO: Ask about DATE OCC or DATE RPRT
    df = df.withColumn("year", col("DATE OCC").substr(7, 4))
    df = df.withColumn("month", col("DATE OCC").substr(0, 2))

    df = df.groupby(col("year"), col("month")).count().withColumnRenamed("count", "crime_total")

    # Define a window specification to partition by year and order by crime_total in descending order
    window_spec = Window.partitionBy("year").orderBy(col("crime_total").desc())

    '''The difference between rank and dense_rank is that dense_rank leaves no gaps in ranking
    sequence when there are ties. That is, if you were ranking a competition using dense_rank
    and had three people tie for second place, you would say that all three were in second
    place and that the next person came in third. Rank would give me sequential numbers, making
    the person that came in third place (after the ties) would register as coming in fifth.'''
    # TODO: Ask about rank or dense rank
    df = df.withColumn("rank", rank().over(window_spec))
    df = df.orderBy(col("year"), col("crime_total").desc())
    df = df.where(col("rank") <= 3)

    df.show(100)


def sql_api(spark, df):
    # Create a temporary view from the DataFrame
    df.createOrReplaceTempView("crime_data")

    # Extract year and month from DATE OCC using SQL
    spark.sql("""
        SELECT 
            *,
            substr(`DATE OCC`, 7, 4) AS year,
            substr(`DATE OCC`, 0, 2) AS month
        FROM crime_data
    """).createOrReplaceTempView("crime_data_with_date")

    # Group by year and month, and count occurrences
    spark.sql("""
        SELECT
            year,
            month,
            COUNT(*) AS crime_total
        FROM crime_data_with_date
        GROUP BY year, month
    """).createOrReplaceTempView("crime_counts")

    top3_months_df = spark.sql("""
        SELECT * FROM (
            SELECT *,
            RANK() OVER (PARTITION BY year ORDER BY crime_total DESC) AS rank
            FROM crime_counts
        ) ranked
        WHERE ranked.rank <= 3
    """)

    # Show the result
    top3_months_df.show(100)

    # return top3_months_df


class Q1:
    def __init__(self,
                 name,
                 csv_path=hdfs_path + "Crime_Data_from_2010_to_2019.csv",
                 csv_path_2=hdfs_path + "Crime_Data_from_2020_to_Present.csv",
                 parquet_path=hdfs_path + "Crime_Data_from_2010_to_Present.parquet"):
        self.spark = SparkSession.builder.appName(name).getOrCreate()
        self.csv_path = csv_path
        self.csv_path_2 = csv_path_2
        self.parquet_path = parquet_path
        self.name = name
        try:
            self.write_parquet()
            print("parquet")
        except Exception as e:
            print(e)

    def write_parquet(self):
        # Read the CSV file into a DataFrame
        df = self.spark.read.csv(self.csv_path, header=True, inferSchema=True)
        df2 = self.spark.read.csv(self.csv_path_2, header=True, inferSchema=True)
        df = df.union(df2)

        # Write the DataFrame to Parquet format
        df.write.parquet(self.parquet_path)

    def query(self, file_type="csv", method="sql"):
        if file_type == "csv":
            df = self.spark.read.csv(self.csv_path, header=True, inferSchema=True)
            df2 = self.spark.read.csv(self.csv_path_2, header=True, inferSchema=True)
            df = df.union(df2)
        elif file_type == "parquet":
            df = self.spark.read.parquet(self.parquet_path, header=True, inferSchema=True)
        else:
            raise ValueError("Wrong file type")

        if method == "sql":
            sql_api(self.spark, df)
        elif method == "spark_sql":
            spark_sql(df)
        else:
            raise ValueError("Wrong value")

    def clear_cache(self):
        self.spark.catalog.clearCache()
        self.spark.stop()
        self.spark = SparkSession.builder.appName(self.name).getOrCreate()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # convert_csv_to_parquet("Crime_Data_from_2010_to_2019.csv", "Crime_Data_from_2010_to_2019.parquet")
    import time

    Q1 = Q1("Top3MonthsCrimes")

    file_types = ["parquet", "csv"]
    methods = ["sql", "spark_sql"]
    dictionary = {}
    for file_type in file_types:
        if file_type not in dictionary:
            dictionary[file_type] = {}

        for method in methods:
            start_time = time.time()
            Q1.query(file_type, method)
            elapsed_time = time.time() - start_time

            Q1.clear_cache()
            print(f"Elapsed Time for {file_type} {method}: {elapsed_time}")
            dictionary[file_type][method] = elapsed_time

    print_dictionary(dictionary)