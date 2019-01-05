from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
import sys
import re, string
conf = SparkConf().setAppName('nasa logs')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+
cluster_seeds = ['199.60.17.188', '199.60.17.216']
spark = SparkSession.builder.appName('Spark Cassandra example').config('spark.cassandra.connection.host',','.join(cluster_seeds)).getOrCreate()
import uuid
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
import os

def main(inputs, keyspace, table):
	text = sc.textFile(inputs).repartition(500)
	logs = text.flatMap(words_once)
	nasa_schema = types.StructType([
	    types.StructField('host', types.StringType(), False),
		types.StructField('id', types.StringType(), False),
		types.StructField('bytes', types.IntegerType(), False),
	])
	dfLogs = spark.createDataFrame(logs, nasa_schema)
	dfLogs.write.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=keyspace).save()

wordsep = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

def words_once(line):
    w = wordsep.split(line)
    if len(w) > 4:
        yield (w[1], str(uuid.uuid4()), int(w[4]))

if __name__ == '__main__':
	inputs = sys.argv[1]
	keyspace = sys.argv[2]
	table = sys.argv[3]
	main(inputs, keyspace, table)

