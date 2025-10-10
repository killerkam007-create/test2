from pyspark.sql import SparkSession
print("test 2 iniciated")
spark=SparkSession.builder.appName("TestApp").getOrCreate()
print("test 2 completed")
df = spark.read.csv(
    "C:\\Users\\SURAJ\\OneDrive\\Desktop\\pro\\test2\\cities.csv",
    header=True,
    inferSchema=True
)
# df.show()
df.printSchema()
df.columns
df.df["City"].show()

spark.stop()