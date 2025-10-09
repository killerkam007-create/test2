from pysaprk.sql import SparkSession
print("test 2 iniciated")
spark=SparkSession.builder.appName("TestApp").getOrCreate()
print("test 2 completed")