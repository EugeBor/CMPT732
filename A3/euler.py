from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import re, string
import json
import operator
import random
import time

def main(partition):
	random.seed()
	total_iterations = 0
	for i in range(partition):
		sum = 0
		while sum < 1.0:
			addition = random.random()
			sum = sum + addition
			total_iterations = total_iterations + 1
	return total_iterations		

if __name__ == '__main__':
	conf = SparkConf().setAppName('reddit average')
	sc = SparkContext(conf=conf)
	assert sc.version >= '2.3'  # make sure we have Spark 2.3+
	samples = int(sys.argv[1])
	slices = int(sys.argv[2])
	start_time = time.time()
	total_iterations = 0
	par = sc.parallelize([samples // slices] * slices, slices)
	total = par.map(main).reduce(operator.add)

	print("----------> The Average is " + str(total/samples))
	print("----------> %s seconds" % (time.time() - start_time))
	print("----------> " + str(samples/1000000) + " million")
	print("----------> # of slices = " + str(slices))