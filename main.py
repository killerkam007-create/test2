from sre_parse import State
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import count,sum,lower,upper,avg,udf,lit,when
from pyspark.sql import SparkSession,Window
from dict_cal_value import dict_cal
from test2 import test,test_init_upper
from read_data import read_data1
from pyspark.sql import functions as F
from mics import reverse_string,duplicateValue,preservingOrderList
from mics import factorial_test,nth_largest_number,flatten_lsit,count_frequency,reverse_mics
def count_words(a):
    word=a.split()
    for item in word:
      word_count=len(item)
      print(f"Total number of words in the given string is: {word_count}")
    return
    
     
def count_value(a):
    b=set(a)
    for item in b:
        j=a.count(item)
        print(f"Element \"{item}\" appears {j} times.")    
    return j
def flatten_list(nested_list):
    planned_list = []
    print(f"Processing nested list: {planned_list}")
    for item in nested_list:
        if isinstance(item, list):
            print(f"Found inner list: {item}")
            planned_list.extend(flatten_list(item))  # add all elements of the inner list
        else:
            planned_list.append(item) 
    return planned_list

if __name__=="__main__":
  d1 = {'a': 1, 'b': 2}
  d2 = {'b': 3, 'c': 4}

  d1.update(d2)  # modifies d1
  print(d1)       # {'a': 1, 'b': 3, 'c': 4}

  for key, value in d1.items():
      print(f"{key}: {value}")

 




#     number_dict = {
#     1: "one",
#     2: "two",
#     3: "three",
#     4: "four",
#     5: "five",
#     6: "six",
#     7: "seven",
#     8: "eight",
#     9: "nine",
#     10: "ten"
# }

# print(number_dict)

# keys = ['a', 'b', 'c']
# values = [1, 2, 3]
# dict1={}
# for i in range(len(keys)):
#     dict1[keys[i]]=values[i]
# print(dict1) 
# dict2={}
# for key in dict1:
#   a=dict1[key]
#   dict2[a]=key
# print(dict2)
# k = ['a', 'b', 'c',2,3,4,5,6,7,8,9,10]
# a=[item for item in k if (isinstance(item,int) and item%2==0)]
# print(a)
# arr = ['a', 'b', 'a', 'c', 'b', 'a']
# freq = {}
# z=set(arr)
# for item in z:
#    freq[item]=arr.count(item)
# print(freq)
# a='comprehensive list of 50 interview questions tailored for a Data Engineer with 5+ years of experience, covering SQL, data modeling, ETL, big data, cloud platforms, and system design.'
# b=a.split(" ") 
# z=set(b)
# print(b)
# c=[item for item in b if item.endswith("0")]
# print(c)
# dict1={}
# for item in z:
#    dict1[item]=b.count(item)
# print(dict1)
  nested_list = [1, 2, [3, [4, 5,9],0], 6, ['a', 'b']]
  planned_list=flatten_list(nested_list)
  print(planned_list)

# print(planned_list)


    # b=a.split()
    # count_words(a)
    # count_value(b)
    # print("--------------------------------------------------")
    
    # reverse_string(a)
    # print(list(a))
    # sample_list=[1,9,2,3,3,4,55,5,6,7,8,9,9,10]
    # total=0
  # Given a list of numbers, find the sum without using sum()
    # for item in sample_list:
    #   total+=item
    # print(f"Sum of elements in the list is: {total}")
    # Find the largest and smallest element in a list.
    # largest_element=max(sample_list)
    # print(f"Largest element in the list is: {largest_element}")
    # smallest_element=min(sample_list)
    # print(f"Smallest element in the list is: {smallest_element}")
    # # Count how many times a specific element appears.
    # ina=9
    # count_of_val=count_frequency(sample_list,ina)
    # print(f"Element {ina} appears {count_of_val} times in the list.")
    # # Reverse a list without using reverse().
    # reversed_list=reverse_mics(sample_list)
    # print(f"Reversed list is: {reversed_list}")
    # # Remove duplicate elements from a list.
    # print(f'Removing duplicates from {sample_list} is  {set(sample_list)}')
    # # use map function to do update.
    # l1=[1,2,3,4,5,6,7,8,9,10]
    # square_list=list(map(lambda x: x*x,l1))
    # print(f"Square of list elements using map function: {square_list}")
    

    # spark=SparkSession.builder.appName("TestApp").getOrCreate()
    # df=read_data1(spark)
    # udf_funct=udf()
    # df.show()
    # df2=df.select("Pincode","Location")
    # df2.show()
    # df3=df.filter(df["Pincode"]==110092)
    # df3.show()
    # df4=df.withColumn("City",upper(df["Location"]))
    # df4.show(5,truncate=False)
    # df5=df.filter(lower(df["Location"]).endswith("ar"))
    # df5.show()
    # df6=df.groupby(
    #     df["State"]).agg(
    #         count("Pincode").alias("Pincode_Count"),
    #         sum(df["Pincode"]).alias("Pincode_Sum"),
    #         avg(df["Pincode"]).alias("Pincode_Avg")
    #             ).orderBy(df["State"])
    # df6=df6.dropna()
    # df6=df6.withColumn("flag_code",when(df6["Pincode_Count"]<=100,"Low")
    #                    .when((df6["Pincode_Count"]>100) & (df6["Pincode_Count"]<500),"Medium")
    #                    .otherwise("High")
    #                    )
    # df6.show(truncate=False)
    # win_spc=Window.partitionBy(df6["flag_code"]).orderBy(df6["flag_code"])
    # df7=df6.withColumn("row_num",F.row_number().over(win_spc))
    # df7.show(100,truncate=False)
    # df7=df7.withColumn("State",F.trim(df7["State"]))
    # df_summary=df7.withColumn("Summary",F.concat_ws("_",df7["State"],df7["Pincode_Count"],df7["Flag_code"],df7["row_num"]))
    # df_summary.show(100)

    # dfasssam=df.filter(df["State"]=="Assam")
    # df_pin=dfasssam.select("Pincode","Location")
    # df_pin=df_pin.withColumn("test_date",lit("2024-01-01"))
    # df_Dict=dfasssam.select("District","State","Pincode")
    # df_final=df_pin.join(broadcast(df_Dict),on="Pincode",how="inner") 
    # df_final.show(truncate=False)



    # df7 = df6.withColumn("f_name", F.split(F.col("State"), " ").getItem(0)) \
    #      .withColumn("l_name", F.split(F.col("State"), " ").getItem(1))
    # df7.show()


    


    # df=df.withColumn("State",upper(df["State"]))
    # df=df.withColumn("City",lower(df["City"]))
    # assam_df = (
    # df.filter(df["State"] == "Assam")
    #   .groupBy("District")
    #   .agg(
    #       count("Pincode").alias("count"),
    #       sum("Pincode").alias("pin_sum")
    #   )).orderBy("District")
    # assam_df.show()
    # assam_df=assam_df.withColumn("Result",udf(test)("count"))
    # df.show()
    # # assam_df.write.csv("C:\\Users\\SURAJ\\OneDrive\\Desktop\\pro\\test2\\Assam",header=True)
    # assam_df.coalesce(1).write.option("header", True).mode("overwrite").csv("C:\\Users\\SURAJ\\OneDrive\\Desktop\\pro\\test2\\Assam",header=True)


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
    # spark.stop()


