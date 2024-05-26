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

    def query(self, method=1):
        df_crimes = self.spark.read.csv(self.crimes_csv_path, header=True, inferSchema=True)
        df_crimes = df_crimes.withColumn("year", col("DATE OCC").substr(7, 4))
        df_crimes = df_crimes.where(col("year") == "2015").drop("year")
        df_crimes = df_crimes.filter((df_crimes["Vict Descent"].isNotNull()) & (df_crimes["Vict Descent"] != ""))

        df_income = self.spark.read.csv(self.income_csv_path, header=True, inferSchema=True)
        df_geolocation = self.spark.read.csv(self.revgecoding_csv_path, header=True, inferSchema=True)

        convert_to_float_udf = udf(lambda x: convert_to_float(x), FloatType())
        df_income = df_income.withColumn("income", convert_to_float_udf(col("Estimated Median Income")))

        df_geolocation = df_geolocation.withColumn("New Zip Code", col("ZIPcode").substr(0, 5))

        col_name = "income"
        if method == 1:
            top3_df = df_income.orderBy(col(col_name).desc()).limit(3)
            bottom3_df = df_income.orderBy(col(col_name).asc()).limit(3)
            filtered_incomes_df = top3_df.union(bottom3_df)
            filtered_incomes_df.show()
        elif method == 2:
            '''
            optimization
            abs(rank - (total + 1)/2).orderby(rank.desc()).limit(6)  
            '''
            total_rows = (df_income.count() + 1) / 2
            windowSpec = Window.orderBy(col("income").asc())
            ranked = df_income.withColumn("rank", F.row_number().over(windowSpec))
            custom_ranked = ranked.withColumn("custom_rank", F.abs(col("rank") - total_rows))
            filtered_incomes_df = custom_ranked.orderBy(col("custom_rank").desc()).limit(6)
            filtered_incomes_df.show()
        else:
            raise ValueError("Not implemented")

        # We have filtered_incomes_df
        # We need to join with filtered_incomes_df
        filtered_incomes_df = filtered_incomes_df.distinct()
        filtered_incomes_df.show()

        # Inner join between incomes and geolcation
        geocoordinates_df = filtered_incomes_df.join(
            df_geolocation,
            filtered_incomes_df["Zip Code"] == df_geolocation["New Zip Code"],
            "inner").distinct()

        # geocoordinates_df = result_df.select("LAT", "LON")
        print("geocoordinates_df")
        geocoordinates_df.show()


        # Step 3: join with df_crimes
        crimes_df = df_crimes.join(
            geocoordinates_df,
            (geocoordinates_df["LAT"] == df_crimes["LAT"]) & (geocoordinates_df["LON"] == df_crimes["LON"]),
            "left"
        ).distinct()

        convert_to_color_udf = udf(lambda x: convert_code_to_descent(x))
        result = crimes_df.withColumn("descent", convert_to_color_udf(col("Vict Descent"))).groupBy("descent").count().orderBy(col("count").desc())

        result.show()

    def clear_cache(self):
        self.spark.catalog.clearCache()
