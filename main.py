from pyspark.sql.functions import count,sum,lower,upper,avg,udf
from pyspark.sql import SparkSession
from dict_cal_value import dict_cal
from test2 import test,test_init_upper
from read_data import read_data1
from mics import reverse_string,duplicateValue,preservingOrderList
from mics import factorial_test,nth_largest_number,flatten_lsit,count_frequency,reverse_mics


if __name__=="__main__":
    a='comprehensive list of 50 interview questions tailored for a Data Engineer with 5+ years of experience, covering SQL, data modeling, ETL, big data, cloud platforms, and system design.'
    reverse_string(a)
    print(list(a))
    sample_list=[1,9,2,3,3,4,55,5,6,7,8,9,9,10]
    total=0
  # Given a list of numbers, find the sum without using sum()
    for item in sample_list:
      total+=item
    print(f"Sum of elements in the list is: {total}")
    # Find the largest and smallest element in a list.
    largest_element=max(sample_list)
    print(f"Largest element in the list is: {largest_element}")
    smallest_element=min(sample_list)
    print(f"Smallest element in the list is: {smallest_element}")
    # Count how many times a specific element appears.
    ina=9
    count_of_val=count_frequency(sample_list,ina)
    print(f"Element {ina} appears {count_of_val} times in the list.")
    # Reverse a list without using reverse().
    reversed_list=reverse_mics(sample_list)
    print(f"Reversed list is: {reversed_list}")
    # Remove duplicate elements from a list.
    print(f'Removing duplicates from {sample_list} is  {set(sample_list)}')




    # 
    # spark=SparkSession.builder.appName("TestApp").getOrCreate()
    # df=read_data1(spark)
    # udf_funct=udf()
    # # df.show()
    # df.printSchema()
    # df.columns
    # # df=df.withColumn("State",upper(df["State"]))
    # # df=df.withColumn("City",lower(df["City"]))
    # assam_df = (
    # df.filter(df["State"] == "Assam")
    #   .groupBy("District")
    #   .agg(
    #       count("Pincode").alias("count"),
    #       sum("Pincode").alias("pin_sum")
    #   )).orderBy("District")
    # assam_df.show()
    # assam_df=assam_df.withColumn("Result",udf(test)("count"))
    # assam_df.show()
    # # assam_df.write.csv("C:\\Users\\SURAJ\\OneDrive\\Desktop\\pro\\test2\\Assam",header=True)
    # assam_df.coalesce(1).write.option("header", True).mode("overwrite").csv("C:\\Users\\SURAJ\\OneDrive\\Desktop\\pro\\test2\\Assam",header=True)


    # # assam_df.write \
    # # .format("csv") \
    # # .mode("overwrite") \
    # # .option("header", "true") \
    # # .save(r"C:\Users\SURAJ\OneDrive\Desktop\pro\test2\Assam")
    # # Partitioning data based on State column
    # # df.write \
    # # .format("csv") \
    # # .mode("overwrite") \
    # # .option("header", "true") \
    # # .partitionBy("State") \
    # # .save(r"C:\Users\SURAJ\OneDrive\Desktop\pro\test2\Assam_partitioned")
    # spark.stop()


