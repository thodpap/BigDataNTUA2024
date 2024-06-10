def Q1_sol():
    from q1 import Q1
    import time

    import datetime
    Q1 = Q1("Top3MonthsCrimes")

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

    Q2 = Q2("CrimesPerDayType")

    print("First execution - doesn't count")
    Q2.query("csv", "spark_sql")
    Q2.clear_cache()

    print("START EXPERIMENT")
    iterations = 1
    # Run multiple times to see actual time
    avg = []
    for i in range(iterations):
        start_time = time.time()
        Q2.query("csv", "spark_sql", False)
        elapsed_time = time.time() - start_time
        avg.append(elapsed_time)
        Q2.clear_cache()
    print(f"Elapsed Time for csv spark_sql (run filter after): {sum(avg) / len(avg)}")

    avg = []
    for i in range(iterations):
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

def Q3_sol():
    from q3 import Q3
    import time
    Q3 = Q3("Q3")
    join_operations = ["broadcast"] #, "merge", "shuffle_hash", "shuffle_replicate_nl"] # "broadcast", "merge", "shuffle_hash", "shuffle_replicate_nl"]
    for join_operation in join_operations:
        start_time = time.time()
        Q3.query(join_operator=join_operation)
        elapsed_time = time.time() - start_time
        print(f"Elapsed Time for csv {join_operation}: {elapsed_time}")

        Q3.clear_cache()

    '''
    1st run
    Elapsed Time for csv broadcast: 11.144367456436157
    Elapsed Time for csv merge: 6.682438611984253
    Elapsed Time for csv shuffle_hash: 6.140656471252441
    Elapsed Time for csv shuffle_replicate_nl: 6.220264911651611
    
    2nd run
    Elapsed Time for csv broadcast: 5.889440059661865
    Elapsed Time for csv merge: 5.74584174156189
    Elapsed Time for csv shuffle_hash: 5.835217475891113
    Elapsed Time for csv shuffle_replicate_nl: 5.7729105949401855
    '''
    

def Q4_sol():
    from q4 import Q4
    import time

    Q4 = Q4("Q4")

    dictionary = {}
    for join_type in ["broadcast", "repartition", "none"]:
        if join_type not in dictionary:
            dictionary[join_type] = {}
        for type_ in ["rdd", "spark"]:
            start_time = time.time()
            Q4.query(join_type, type_)
            elapsed_time = time.time() - start_time
            print(f"Elapsed Time for csv rdd with {type_}: {elapsed_time}")
            Q4.clear_cache()
            dictionary[join_type][type_] = elapsed_time

    from q4 import print_dictionary
    print_dictionary(dictionary)

if __name__ == '__main__':
    Q4_sol()
