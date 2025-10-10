# from pysaprk.sql import SparkSession
# print("test 2 iniciated")
# spark=SparkSession.builder.appName("TestApp").getOrCreate()
# print("test 2 completed")
import kagglehub

# Download latest version
path = kagglehub.dataset_download("tariqbashir/cities-of-india-with-pin-codes")

print("Path to dataset files:", path)