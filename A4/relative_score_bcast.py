from pyspark import SparkConf, SparkContext
import sys
import re, string
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json

def main(inputs, output):
    text = sc.textFile(inputs)
    views_all = text.map(reddits).cache()
    calculate = views_all.reduceByKey(add_pairs).map(calculate_average).filter(lambda x: x[1] > 0)
    dictionary = sc.broadcast(dict(calculate.collect()))
    outdata = views_all.map(lambda x: bcast_best(x, dictionary)).map(json.dumps)
    outdata.saveAsTextFile(output)

def reddits(line):
    x = json.loads(line)
    name = x["subreddit"]
    author = x["author"]
    count = 1
    score = x["score"] 
    return (name, (count, score, author))

def add_pairs(current, cumulative):
    result = (cumulative[0] + current[0], cumulative[1] + current[1])
    return result

def get_key(x):
	return x[2]

def calculate_average(kvx):
	return (kvx[0], kvx[1][1] / kvx[1][0])

def calculate_best(kvx):
	return (kvx[0], kvx[1][1] / kvx[1][0])

def bcast_best(x, bcast):
    return (x[0], x[1][2], bcast.value(x[0]))

def output_format(kvx):
    return json.dumps(kvx)

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit average')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
