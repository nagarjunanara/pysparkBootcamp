from pyspark import SparkConf, SparkContext
import configparser as cp
import sys

props = cp.ConfigParser()

print(props.read
      ('C:\\Users\\nara_\\PycharmProjects\\BootCampPyspark\\src\\resources\\application.properties'))
print("Sections : ", props.sections())
env = sys.argv[1]
month= sys.argv[2]
conf = SparkConf(). \
setMaster(props.get(env, 'executionMode')). \
    setAppName('RevenueforGivenMonth')
sc = SparkContext(conf=conf)
orders = sc.textFile(props.get(env, 'input.base.dir') + "/orders")
orderitems=sc.textFile(props.get(env,'input.base.dir')+"/order_items")
products=sc.textFile(props.get(env,'input.base.dir')+"/products")
ordersfiltered=orders.filter(lambda order: month in order.split(',')[1])
ordersfilteredTuple=ordersfiltered.map(lambda x:(int(x.split(',')[0]),1))
orderitemsTuple=\
    orderitems.map(lambda oi:(int(oi.split(',')[1]),(int(oi.split(',')[3]),float(oi.split(',')[4]))))
orderJoinOrderItem=orderitemsTuple.join(ordersfilteredTuple)
orderJoinOrderItemRBK=orderJoinOrderItem.map(lambda x:x[1][0]).reduceByKey(lambda x,y:x+y)
productsmap=products.map(lambda p:(int(p.split(',')[0]),p.split(',')[2]))
productsmapjoinOrd=productsmap.join(orderJoinOrderItemRBK)
prodnameRevenue=productsmapjoinOrd.map(lambda n:n[1])
prodnameRevenueascsv=prodnameRevenue.map(lambda v: v[0] + ','+str(v[1]))
prodnameRevenueascsv.saveAsTextFile(props.get(env,'output.base.dir')+'/RevperInMnth.csv')
#print (prodnameRevenue.take(5))
