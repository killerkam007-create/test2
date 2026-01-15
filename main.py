from pyspark.sql.functions import count,sum,lower,upper,avg,udf
from pyspark.sql import SparkSession
from dict_cal_value import dict_cal
from test2 import test,test_init_upper
from read_data import read_data1
from mics import reverse_string,duplicateValue,preservingOrderList
from mics import factorial_test,nth_largest_number,flatten_lsit


if __name__=="__main__":
    a='comprehensive list of 50 interview questions tailored for a Data Engineer with 5+ years of experience, covering SQL, data modeling, ETL, big data, cloud platforms, and system design.'
    reverse_string(a)
    sample_list=[1,2,3,4,5,1,2,3,6,7,8,9,4,5]
    nth_largest_number(sample_list,4)
    flatten_lsit([1,2,[3,4],5,[6,7,8],9])
    preservingOrderList(sample_list)
    duplicateValue(sample_list)
    print(factorial_test(5))
    sales_data = [ {"product": "Laptop", "category": "Electronics", "price": 1000, "quantity": 2}, {"product": "Mouse", "category": "Electronics", "price": 25, "quantity": 5}, {"product": "Desk", "category": "Furniture", "price": 300, "quantity": 1}, {"product": "Chair", "category": "Furniture", "price": 150, "quantity": 4} ]
    calValue=dict_cal(sales_data)
    print(calValue)
    spark=SparkSession.builder.appName("TestApp").getOrCreate()
    df=read_data1(spark)
    udf_funct=udf()
    # df.show()
    df.printSchema()
    df.columns
    # df=df.withColumn("State",upper(df["State"]))
    # df=df.withColumn("City",lower(df["City"]))
    assam_df = (
    df.filter(df["State"] == "Assam")
      .groupBy("District")
      .agg(
          count("Pincode").alias("count"),
          sum("Pincode").alias("pin_sum")
      )).orderBy("District")
    assam_df.show()
    assam_df=assam_df.withColumn("Result",udf(test)("count"))
    assam_df.show()
    # assam_df.write.csv("C:\\Users\\SURAJ\\OneDrive\\Desktop\\pro\\test2\\Assam",header=True)
    assam_df.coalesce(1).write.option("header", True).mode("overwrite").csv("C:\\Users\\SURAJ\\OneDrive\\Desktop\\pro\\test2\\Assam",header=True)


    # assam_df.write \
    # .format("csv") \
    # .mode("overwrite") \
    # .option("header", "true") \
    # .save(r"C:\Users\SURAJ\OneDrive\Desktop\pro\test2\Assam")
    # Partitioning data based on State column
    # df.write \
    # .format("csv") \
    # .mode("overwrite") \
    # .option("header", "true") \
    # .partitionBy("State") \
    # .save(r"C:\Users\SURAJ\OneDrive\Desktop\pro\test2\Assam_partitioned")
    spark.stop()


