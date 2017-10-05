from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals

import sys
from pyspark.sql import SparkSession

def getSpark():
    spark = SparkSession \
        .builder \
        .appName("Pyspark entry point") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    return spark

if __name__ == '__main__':
    output_path = sys.argv[1]

    spark = getSpark()
    spark.sql("show databases").show()

    print("Start dump df to {}".format(output_path))
    spark.sql("show databases").write.mode("overwrite").parquet(output_path)
    print("Finish write dataframe!")

    spark.stop()