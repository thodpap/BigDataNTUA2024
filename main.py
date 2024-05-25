# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, rank


def Q1_pandas_group_by_year_by_crimes_rank():
    spark = SparkSession.builder.appName("Top3MonthsCrimes").getOrCreate()

    df = spark.read.csv("data/Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)

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

    return df

def Q1_parquet_sql():
    spark = SparkSession.builder.appName("Top3MonthsCrimes").getOrCreate()

    df = spark.read.csv("data/Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    Q1_pandas_group_by_year_by_crimes_rank()


    # Filter
    # df.select("year").where(col("year") == "2010").show(15)
