def Q1():
    from q1_sql import Q1
    Q1 = Q1("Top3MonthsCrimes", "data/Crime_Data_from_2010_to_2019.csv", "data/Crime_Data_from_2010_to_2019.parquet")

    file_types = ["parquet", "csv"]
    methods = ["sql", "spark_sql"]
    for file_type in file_types:
        for method in methods:
            print(file_type, method)
            Q1.query(file_type, method)


if __name__ == '__main__':
    Q1()

