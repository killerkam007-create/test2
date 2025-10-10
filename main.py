from pyspark.sql.functions import count,sum
from pyspark.sql import SparkSession
from dict_cal_value import dict_cal



if __name__=="__main__":
    sales_data = [ {"product": "Laptop", "category": "Electronics", "price": 1000, "quantity": 2}, {"product": "Mouse", "category": "Electronics", "price": 25, "quantity": 5}, {"product": "Desk", "category": "Furniture", "price": 300, "quantity": 1}, {"product": "Chair", "category": "Furniture", "price": 150, "quantity": 4} ]
    calValue=dict_cal(sales_data)
    print(calValue)
    spark=SparkSession.builder.appName("TestApp").getOrCreate()
    df = spark.read.csv(
        "C:\\Users\\SURAJ\\OneDrive\\Desktop\\pro\\test2\\cities.csv",
        header=True,
        inferSchema=True
    )
    # df.show()
    df.printSchema()
    df.columns
    assam_df = (
    df.filter(df["State"] == "Assam")
      .groupBy("District")
      .agg(
          count("Pincode").alias("count"),
          sum("Pincode").alias("pin_sum")
      )).orderBy("District")
    assam_df.show()

    assam_df.write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .save(r"C:\Users\SURAJ\OneDrive\Desktop\pro\test2\Assam.csv")
    # Partitioning data based on State column
    assam_df.write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .partitionBy("State") \
    .save(r"C:\Users\SURAJ\OneDrive\Desktop\pro\test2\Assam_partitioned")
    spark.stop()


