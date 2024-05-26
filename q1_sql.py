# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
#!pip install pyspark

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, rank


def sql_(df):
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

    df.show()


class Q1:
    def __init__(self, name, csv_path="data/Crime_Data_from_2010_to_2019.csv", parquet_path="data/Crime_Data_from_2010_to_2019.parquet"):
        self.spark = SparkSession.builder.appName(name).getOrCreate()
        self.csv_path = csv_path
        self.parquet_path = parquet_path
        try:
            self.write_parquet()
        except Exception as e:
            print(e)

    def write_parquet(self):
        # Read the CSV file into a DataFrame
        df = self.spark.read.csv(self.csv_path, header=True, inferSchema=True)

        # Write the DataFrame to Parquet format
        df.write.parquet(self.parquet_path)

    def csv_sql(self):
        df = self.spark.read.csv("data/Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)
        sql_(df)

    def parquet_sql(self):
        df = self.spark.read.parquet("data/Crime_Data_from_2010_to_2019.parquet", header=True, inferSchema=True)
        sql_(df)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # convert_csv_to_parquet("data/Crime_Data_from_2010_to_2019.csv", "data/Crime_Data_from_2010_to_2019.parquet")
    Q1 = Q1("Top3MonthsCrimes", "data/Crime_Data_from_2010_to_2019.csv", "data/Crime_Data_from_2010_to_2019.parquet")
    print("Parquet")
    Q1.parquet_sql()

    print("CSV")
    Q1.csv_sql()

    # Filter
    # df.select("year").where(col("year") == "2010").show(15)
