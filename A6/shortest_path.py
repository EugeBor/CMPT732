from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
import sys, os
import re, string
conf = SparkConf().setAppName('shortest path')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

def getEdges(line):
	w = line.split()
	g = []
	for i in range(1, len(w)):
		g.append(w[i])
	return (w[0][:-1], g)

def getLayer(line):
	yield (line[0], line[1][1])
	i = 1
	while i <= len(line[1][0]):
		yield (line[1][0][i-1], (line[0], int(line[1][1][1])+1))
		i+=1

def getKey(line):
	return line[0]


def main(inputs, output, start, finish):
	text = sc.textFile(os.path.join(inputs, 'links-simple-sorted.txt'))
	edges = text.map(getEdges).cache()

	paths = sc.parallelize([(start, ("-", 0))])
	i = 1
	while i <= 6:
		prevPaths = paths
		paths = edges.join(prevPaths).flatMap(getLayer).reduceByKey(min).cache()
		i += 1
		paths.saveAsTextFile(output + '/iter-' + str(i-1))
		if paths.lookup(finish):
			break

	path = [finish]
	if len(paths.lookup(finish))>0:
		path.append(paths.lookup(finish)[0][0])
		prevNode = paths.lookup(finish)[0][0]
		if path != finish:
			i = 1
			while i <= 6:
				prevNode = paths.lookup(prevNode)[0][0]
				path.append(prevNode)
				if (i == 6 and prevNode != start):
					path = ["no path exists"]
					break
				i += 1
				if prevNode == start:
					break
	else:
		path = ["no path exists"]
	path = sc.parallelize(path[::-1], 1)
	path.saveAsTextFile(output + '/path')

if __name__ == '__main__':
	inputs = sys.argv[1]
	output = sys.argv[2]
	start = sys.argv[3]
	finish = sys.argv[4]
	main(inputs, output, start, finish)