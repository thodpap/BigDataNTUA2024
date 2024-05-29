'''
1. Broadcast Join (broadcast):

    Used when: One side of the join is significantly smaller than the other.
    Advantages: Minimizes the data shuffled between nodes because the smaller dataset is broadcasted to all nodes.
    Limitations: Not suitable if the smaller dataset isn't small enough to fit in memory.
    Performance: In your logs, hints to use broadcast were not supported due to the nature of the join, possibly indicating that the data was too large or not appropriate for a broadcast join.

2. Sort Merge Join (sortmerge):

    Used when: Both datasets are large, and neither can be efficiently broadcasted.
    Advantages: Efficient for large datasets as it involves sorting data on the join keys and then merging, which is generally scalable.
    Limitations: High overhead due to sorting and shuffling data.
    Performance: This seems to be Spark's fallback method when other hints are not followed, suggesting it's the safest option for large datasets.

3. Shuffle Hash Join (shuffle_hash):

    Used when: Both datasets are large, but manageable enough that a hash table can be built for at least one side.
    Advantages: Can be faster than sort merge joins if one side of the join is moderately sized because it avoids sorting.
    Limitations: Requires sufficient memory to maintain a hash table of one of the datasets.
    Performance: Warnings in your logs suggest that Spark did not apply this hint, possibly due to memory constraints or size issues.

4. Shuffle Replicate NL Join (shuffle_replicate_nl):

    Used when: You need to force a nested loop join, typically used for Cartesian products.
    Advantages: Necessary when specific non-equi joins or complex conditions are involved.
    Limitations: Highly inefficient for large datasets due to the nature of nested loops.
    Performance: It's the least efficient for large datasets and suitable for specific scenarios only.


'''
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, udf, row_number
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

def convert_to_float(num):
    return float(num.replace('$', '').replace(',', ''))


def convert_code_to_descent(code):
    descent_code_mapping = {
        "A": "Other Asian",
        "B": "Black",
        "C": "Chinese",
        "D": "Cambodian",
        "F": "Filipino",
        "G": "Guamanian",
        "H": "Hispanic/Latin/Mexican",
        "I": "American Indian/Alaskan Native",
        "J": "Japanese",
        "K": "Korean",
        "L": "Laotian",
        "O": "Other",
        "P": "Pacific Islander",
        "S": "Samoan",
        "U": "Hawaiian",
        "V": "Vietnamese",
        "W": "White",
        "X": "Unknown",
        "Z": "Asian Indian"
    }
    return descent_code_mapping.get(code, "")


class Q3:
    def __init__(self, name,
                 crimes_csv_path="data/Crime_Data_from_2010_to_2019.csv",
                 income_csv_path="data/income/LA_income_2015.csv",
                 revgecoding_csv_path="data/revgecoding.csv"):
        self.spark = SparkSession.builder.appName(name).getOrCreate()
        self.crimes_csv_path = crimes_csv_path
        self.income_csv_path = income_csv_path
        self.revgecoding_csv_path = revgecoding_csv_path
        self.name = name

    def read_datasets(self):
        df_crimes = self.spark.read.csv(self.crimes_csv_path, header=True, inferSchema=True)

        # Filter only for year 2015 & Accept only those whose origin is known
        df_crimes = df_crimes.withColumn("year", col("DATE OCC").substr(7, 4))
        df_crimes = df_crimes.where(col("year") == "2015").drop("year")
        df_crimes = df_crimes.filter((df_crimes["Vict Descent"].isNotNull()) & (df_crimes["Vict Descent"] != ""))

        df_income = self.spark.read.csv(self.income_csv_path, header=True, inferSchema=True)
        df_geolocation = self.spark.read.csv(self.revgecoding_csv_path, header=True, inferSchema=True)

        convert_to_float_udf = udf(lambda x: convert_to_float(x), FloatType())
        df_income = df_income.withColumn("income", convert_to_float_udf(col("Estimated Median Income")))

        # Keep only first zip code into a new column named "New Zip Code"
        df_geolocation = df_geolocation.withColumn("New Zip Code", col("ZIPcode").substr(0, 5))

        return df_crimes, df_income, df_geolocation

    def get_bottom_and_higher_3(self, df_income, col_name, method=1):
        if method == 1:
            top3_df = df_income.orderBy(col(col_name).desc()).limit(3)
            bottom3_df = df_income.orderBy(col(col_name).asc()).limit(3)
            filtered_incomes_df = top3_df.union(bottom3_df)
            filtered_incomes_df.show()
        elif method == 2:
            """
            optimization
            abs(rank - (total + 1)/2).orderby(rank.desc()).limit(6)
            """
            total_rows = (df_income.count() + 1) / 2
            window_spec = Window.orderBy(col("income").asc())
            ranked = df_income.withColumn("rank", F.row_number().over(window_spec))
            custom_ranked = ranked.withColumn("custom_rank", F.abs(col("rank") - total_rows))
            filtered_incomes_df = custom_ranked.orderBy(col("custom_rank").desc()).limit(6)
            filtered_incomes_df.show()
        else:
            raise ValueError("Not implemented")

    def get_bottom_3(self, df_income, col_name):
        return df_income.orderBy(col(col_name).asc()).limit(3)

    def get_upper_3(self, df_income, col_name):
        return df_income.orderBy(col(col_name).asc()).limit(3)

    def filter_by_double_zipcodes(self, df_incomes, df_geolocation):
        # Inner join between incomes and geolocation
        df_geocoordinates = df_incomes.join(
            df_geolocation,
            df_incomes["Zip Code"] == df_geolocation["New Zip Code"],
            "inner").distinct()

        return df_geocoordinates

    def convert_to_descent(self, df_crimes, df_geocoordinates, join_operator=""):
        # Step 3: join with df_crimes
        print("Convert_to_descent", df_crimes.count(), df_geocoordinates.count())

        if len(join_operator) > 0:
            df_crimes = df_crimes.hint(join_operator)

        crimes_df = df_crimes.join(
            df_geocoordinates,
            (df_geocoordinates["LAT"] == df_crimes["LAT"]) & (df_geocoordinates["LON"] == df_crimes["LON"]),
            "left"
        ).distinct()
        crimes_df.explain()

        convert_to_color_udf = udf(lambda x: convert_code_to_descent(x))
        result = crimes_df.withColumn("descent", convert_to_color_udf(col("Vict Descent"))).groupBy(
            "descent").count().orderBy(col("count").desc())

        return result

    def query(self, method=1, join_operator=""):
        df_crimes, df_income, df_geolocation = self.read_datasets()

        col_name = "income"
        df_income_high_3 = self.get_upper_3(df_income, col_name)
        df_income_bottom_3 = self.get_bottom_3(df_income, col_name)

        df_geocoordinates_high = self.filter_by_double_zipcodes(df_income_high_3, df_geolocation)
        df_geocoordinates_low = self.filter_by_double_zipcodes(df_income_bottom_3, df_geolocation)

        descent_high = self.convert_to_descent(df_crimes, df_geocoordinates_high, join_operator)
        descent_low = self.convert_to_descent(df_crimes, df_geocoordinates_low, join_operator)

        print("High income Results")
        descent_high.show()

        print("Low income Results")
        descent_low.show()

    def clear_cache(self):
        self.spark.catalog.clearCache()
        self.spark.sparkContext.stop()
        self.spark.stop()

        self.spark = SparkSession.builder.appName(self.name).getOrCreate()
