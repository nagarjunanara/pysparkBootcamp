from pyspark.sql import SparkSession

spark=SparkSession.builder.master('local').appName('Dataframes_practice').getOrCreate()
orderDF=spark.read.csv('C:\\Users\\nara_\\OneDrive\\Documents\\data-master\\data-master\\retail_db\\orders')
orderDF.show()
spark.read.jdbc("")