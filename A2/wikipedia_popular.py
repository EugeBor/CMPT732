from pyspark import SparkConf, SparkContext
import sys
import re, string

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('wikipedia popular')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

wordsep = re.compile(r'[\s]+')

def words_once(line):
    w = wordsep.split(line)
    return (w[0], w[1], w[2], int(w[3]), w[4])

def get_kvp(kvx):
	return (kvx[0], (kvx[3], kvx[2]))

def get_key(kvx):
	return kvx[0]

def output_format(kvx):
    k = kvx[0]
    v = kvx[1]
    return '%s (%i %s)' % (k, v[0], v[1])

def select_filtered(kvx):
	if kvx[1] == "en" and kvx[2] != "Main_Page" and not kvx[2].startswith("Special:"):
		return True

text = sc.textFile(inputs)
views_all = text.map(words_once)
filtered = views_all.filter(select_filtered)
views_kvp = filtered.map(get_kvp)
views = views_kvp.reduceByKey(max)
outdata = views.sortBy(get_key).map(output_format)
outdata.saveAsTextFile(output)