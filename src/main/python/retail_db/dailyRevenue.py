from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local[2]").setAppName("Daily revenue").set("spark.ui.port",'4004')
sc=SparkContext(conf=conf)
orders=sc.textFile('C:\\Users\\nara_\\OneDrive\\Documents\\data-master\\data-master\\retail_db\\orders')
print (orders.take(10))