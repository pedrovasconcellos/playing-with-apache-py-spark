from pyspark import SparkContext
import os

ip = os.popen("ifconfig en0 | grep inet | grep -v inet6 | awk '{print $2}'").read().strip()
os.environ['SPARK_LOCAL_IP'] = ip
#bash> export SPARK_LOCAL_IP=$(ifconfig en0 | grep inet | grep -v inet6 | awk '{print $2}')

sc = SparkContext()
sc.setLogLevel("OFF")

data = [1,2,3,4,5]
rdd = sc.parallelize(data)

print("\nProcessing started.")
print(f"SPARK_LOCAL_IP={os.environ.get('SPARK_LOCAL_IP')}\n")

print(f"Original Collection: {rdd.collect()}")
print(f"Count of elements: {rdd.count()}")
print(f"Result: {rdd.reduce(lambda x, y: x + y)}")
print(f"Map func(X + 10): {rdd.map(lambda x: x + 10).collect()}")
print(f"Filter(isPair): {rdd.filter(lambda x: (x % 2) == 0).collect()}")

print(" \nFinished processing.\n")
sc.stop()
