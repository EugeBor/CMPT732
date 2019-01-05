from pyspark import SparkConf, SparkContext
import sys
import re, string
import operator
import time

wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))

def words_once(line):
	for w in wordsep.split(line):
		yield (w.lower(), 1)

def get_key(kv):
    return kv[0]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

def main(inputs, output):
	start_time = time.time()
	text = sc.textFile(inputs).repartition(repartitions)
	words = text.flatMap(words_once)
	wordcount = words.reduceByKey(operator.add)
	run_time = time.time() - start_time
	print("----------> %s seconds" % (run_time))
	outdata = wordcount.sortBy(get_key).map(output_format)
	outdata.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('word count improved')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
    inputs = sys.argv[1]
    output = sys.argv[2]
    repartitions = 8
    main(inputs, output)

