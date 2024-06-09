from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, rank

hdfs_path = 'hdfs://master:9000/datasets/'
if __name__ == '__main__':
    spark = SparkSession.builder.appName('convert_to_parquet').getOrCreate()
    df = spark.read.csv(hdfs_path + 'Crime_Data_from_2010_to_2019.csv', header=True, inferSchema=True)
    df2 = spark.read.csv(hdfs_path + 'Crime_Data_from_2020_to_Present.csv', header=True, inferSchema=True)
    df = df.union(df2)
    df.write.parquet(hdfs_path + 'Crime_Data_from_2010_to_Present.parquet')
    spark.stop()