from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
import os, re, sys, gzip
import uuid

inputs = sys.argv[1]
keyspace = sys.argv[2]
table = sys.argv[3]

cluster = Cluster(['199.60.17.188', '199.60.17.216'])
session = cluster.connect(keyspace)

insert_log = session.prepare("INSERT INTO nasalogs (host, id, bytes) VALUES (?, ?, ?);")
batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

wordsep = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
i = 0

for f in os.listdir(inputs):
    with gzip.open(os.path.join(inputs, f), 'rt', encoding='utf-8') as logfile:
        for line in logfile:
            fields = wordsep.split(line)
            if len(fields) > 4:
                if i < 200:
                    batch.add(insert_log, (fields[1], str(uuid.uuid4()), int(fields[4])))
                    i += 1
                else:
                    session.execute(batch)
                    batch.clear()
                    i = 0