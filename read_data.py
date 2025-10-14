def read_data1(spark):
    df = spark.read.csv(
        "C:\\Users\\SURAJ\\OneDrive\\Desktop\\pro\\test2\\cities.csv",
        header=True,
        inferSchema=True
    )
    return df