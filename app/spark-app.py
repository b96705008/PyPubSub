import numpy.random as rnd
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Pyspark entry point") \
	.master("local[*]") \
	.getOrCreate()

spark.sparkContext.setLogLevel('WARN')

spark.sql("show databases").show()

output_path = "/Users/roger19890107/Developer/main/projects/cathay/hippo/PyPubSub/outputs/{}".format(rnd.randint(100000)) 
print("Start dump df to {}".format(output_path))
spark.sql("show databases").write.mode("overwrite").parquet(output_path)
print("Finish write dataframe!")

spark.stop()
