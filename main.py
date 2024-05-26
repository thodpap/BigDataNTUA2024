def Q1_sql():
    from q1_sql import Q1
    Q1 = Q1("Top3MonthsCrimes", "data/Crime_Data_from_2010_to_2019.csv", "data/Crime_Data_from_2010_to_2019.parquet")
    print("Parquet")
    Q1.parquet_sql()

    print("CSV")
    Q1.csv_sql()

if __name__ == '__main__':
    
