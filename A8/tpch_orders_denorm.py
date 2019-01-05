from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as f
import sys, os, re, string
conf = SparkConf().setAppName('tpch')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+
cluster_seeds = ['199.60.17.188', '199.60.17.216']
spark = SparkSession.builder.appName('Spark Cassandra example').config('spark.cassandra.connection.host',','.join(cluster_seeds)).config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()
from cassandra.cluster import Cluster

def output_line(row):
    namestr = ', '.join(sorted(list(row[2])))
    return 'Order #%d $%.2f: %s' % (row[0], row[1], namestr)

def main(keyspace, outdir, orderkeys):
	df = spark.read.format("org.apache.spark.sql.cassandra").options(table="orders_parts", keyspace=keyspace).load()
	output = df.filter(df['orderkey'].isin(orderkeys)).select('orderkey', 'totalprice', 'part_names').sort('orderkey')
	output.show()
	output.rdd.map(output_line).saveAsTextFile(outdir)

if __name__ == '__main__':
	keyspace = sys.argv[1]
	outdir = sys.argv[2]
	orderkeys = list(map(int, sys.argv[3:]))
	main(keyspace, outdir, orderkeys)

