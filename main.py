def Q1():
    from q1_sql import Q1
    import time

    import datetime
    Q1 = Q1("Top3MonthsCrimes", "data/Crime_Data_from_2010_to_2019.csv", "data/Crime_Data_from_2010_to_2019.parquet")

    file_types = ["parquet", "csv"]
    methods = ["sql", "spark_sql"]
    # file_types = ["parquet"]
    # methods = ["sql"]
    for file_type in file_types:
        for method in methods:
            start_time = time.time()
            Q1.query(file_type, method)
            elapsed_time = time.time() - start_time

            Q1.clear_cache()
            print(f"Elapsed Time for {file_type} {method}: {elapsed_time}")


if __name__ == '__main__':
    Q1()

