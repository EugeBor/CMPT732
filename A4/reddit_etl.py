from pyspark import SparkConf, SparkContext
import sys
import re, string
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json
import time

def reddits(line):
    x = json.loads(line)
    name = x["subreddit"]
    score = x["score"]
    author = x["author"]
    return (name, score, author)

def select_filtered_positive(kvx):
	return 'e' in kvx[0] and kvx[1] > 0

def select_filtered_negative(kvx):
	return 'e' in kvx[0] and kvx[1] <= 0


def output_format(kvx):
    return json.dumps(kvx)

def main(inputs, output):
    start_time = time.time()
    text = sc.textFile(inputs)
    selected = text.map(reddits).cache()
    positive = selected.filter(select_filtered_positive)
    negative = selected.filter(select_filtered_negative)
    positive_outdata = positive.map(json.dumps)
    negative_outdata = negative.map(json.dumps)
    positive_outdata.saveAsTextFile(output + "/positive")
    negative_outdata.saveAsTextFile(output + "/negative")
    print("-----xxxxx-----> RUNNING TIME is " + str(time.time()-start_time) + " seconds")


if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit etl')
    sc = SparkContext(conf=conf)

    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)