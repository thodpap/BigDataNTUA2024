def Q1_sol():
    from q1 import Q1
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


def Q2_sol():
    from q2 import Q2
    import time

    csv_file = "data/Crime_Data_from_2010_to_2019.csv"
    Q2 = Q2("CrimesPerDayType", csv_file)

    Q2.query("csv", "spark_sql")
    Q2.clear_cache()

    iteratios = 1
    # Run multiple times to see actual time
    avg = []
    for i in range(iteratios):
        start_time = time.time()
        Q2.query("csv", "spark_sql", False)
        elapsed_time = time.time() - start_time
        avg.append(elapsed_time)
        Q2.clear_cache()
    print(f"Elapsed Time for csv spark_sql (run filter after): {sum(avg) / len(avg)}")

    avg = []
    for i in range(iteratios):
        start_time = time.time()
        Q2.query("csv", "spark_sql")
        elapsed_time = time.time() - start_time
        avg.append(elapsed_time)
        Q2.clear_cache()
    print(f"Elapsed Time for csv spark_sql: {sum(avg) / len(avg)}")

    start_time = time.time()
    Q2.query("csv", "rdd")
    elapsed_time = time.time() - start_time
    print(f"Elapsed Time for csv rdd: {elapsed_time}")
    Q2.clear_cache()


if __name__ == '__main__':
    Q2_sol()
