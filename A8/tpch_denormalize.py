from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as f
import sys, os, re, string
conf = SparkConf().setAppName('nasa logs')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+
cluster_seeds = ['199.60.17.188', '199.60.17.216']
spark = SparkSession.builder.appName('Spark Cassandra example').config('spark.cassandra.connection.host',','.join(cluster_seeds)).getOrCreate()
from cassandra.cluster import Cluster

def main(keyspace_in, keyspace_out):
	o = spark.read.format("org.apache.spark.sql.cassandra").options(table="orders", keyspace=keyspace_in).load()
	l = spark.read.format("org.apache.spark.sql.cassandra").options(table="lineitem", keyspace=keyspace_in).load()
	p = spark.read.format("org.apache.spark.sql.cassandra").options(table="part", keyspace=keyspace_in).load()
	olp = p.join(o.join(l, ['orderkey']), ['partkey']).sort('name')
	parts = olp.groupby("orderkey").agg(f.collect_set("name").alias("part_names"))
	orders_parts = o.join(parts, ['orderkey'])
	orders_parts.write.format("org.apache.spark.sql.cassandra").options(table="orders_parts", keyspace=keyspace_out).save()

if __name__ == '__main__':
	keyspace_in = sys.argv[1]
	keyspace_out = sys.argv[2]
	main(keyspace_in, keyspace_out)

