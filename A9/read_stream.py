from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as f
import sys, os, re, string
conf = SparkConf().setAppName('streaming')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+
spark = SparkSession.builder.appName('streaming').getOrCreate()
spark.sparkContext.setLogLevel('WARN')


def main(topic):
	messages = spark.readStream.format('kafka').option('kafka.bootstrap.servers', '199.60.17.210:9092,199.60.17.193:9092').option('subscribe', topic).load()
	values = messages.select(messages['value'].cast('string'))
	data = values.withColumn('x', f.split(values['value'], ' ')[0]).withColumn('y', f.split(values['value'], ' ')[1]).withColumn('x**2', f.split(values['value'], ' ')[0]**2).withColumn('y**2', f.split(values['value'], ' ')[1]**2).withColumn('xy', f.split(values['value'], ' ')[0]*f.split(values['value'], ' ')[1]).withColumn('n', f.lit(1))
	sums = data.agg(f.sum(data['x']).alias('x'), f.sum(data['y']).alias('y'),f.sum(data['x**2']).alias('x**2'), f.sum(data['y**2']).alias('y**2'), f.sum(data['xy']).alias('xy'), f.sum(data['n']).alias('n'))
	output = sums.select( \
			 ((sums['xy'] - sums['x']*sums['y']/sums['n'])/(sums['x**2']-sums['x']**2/sums['n'])).alias('beta'), \
			 (sums['y']/sums['n'] - sums['x']/sums['n']*((sums['xy'] - sums['x']*sums['y']/sums['n'])/(sums['x**2']-sums['x']**2/sums['n']))).alias('alpha'))
	stream = output.writeStream.format('console').outputMode('complete').start()
	stream.awaitTermination(600)

if __name__ == '__main__':
	topic = sys.argv[1]
	main(topic)

