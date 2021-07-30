from pyspark import SparkContext,SparkConf
import sys
import configparser as cp

props=cp.ConfigParser()
print(props.read('C:\\Users\\nara_\\PycharmProjects\\BootCampPyspark\\src\\resources\\application.properties'))
print ('sections:',props.sections())

env=sys.argv[1]
month=sys.argv[2]
conf=SparkConf().setMaster(props.get(env,'executionMode')).setAppName('PrcticeApp')
sc=SparkContext(conf=conf)
products=sc.textFile(props.get(env,'input.base.dir')+'/products')
print (products.take(10))