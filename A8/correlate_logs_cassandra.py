from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
import sys, re, string
conf = SparkConf().setAppName('correlation')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+
cluster_seeds = ['199.60.17.188', '199.60.17.216']
spark = SparkSession.builder.appName('Spark Cassandra example').config('spark.cassandra.connection.host',','.join(cluster_seeds)).getOrCreate()
from cassandra.cluster import Cluster


def main(keyspace, table):
	df = spark.read.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=keyspace).load()
	df = df.withColumn('y', df['bytes'].cast('integer'))
	df = df.groupby('host').agg(functions.count('host').alias('x'), functions.sum('y').alias('y'))
	df = df.withColumn('x2', df.x ** 2).withColumn('y2', df.y ** 2).withColumn('xy',df.x * df.y)
	df = df.agg(functions.count('host').alias('n'), functions.sum('x').alias('x'),functions.sum('y').alias('y'), functions.sum('x2').alias('x2'),functions.sum('y2').alias('y2'), functions.sum('xy').alias('xy'))
	df = df.withColumn('r2', (df.n * df.xy - df.x * df.y) ** 2 / ((df.n * df.x2 - df.x ** 2) * (df.n * df.y2 - df.y ** 2))).select('r2')
	df.withColumn('r', df.r2**0.5).show()

if __name__ == '__main__':
	keyspace = sys.argv[1]
	table = sys.argv[2]
	main(keyspace, table)

