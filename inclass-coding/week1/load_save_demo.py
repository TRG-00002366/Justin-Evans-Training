from pyspark import SparkContext

sc=SparkContext("local[*]", " Save and Load")

file_rdd=sc.textFile("1661-8.txt")

print(file_rdd.take(3))

sherlock_rdd = file_rdd.filter(lambda line: "Sherlock" in line)
print(sherlock_rdd.take(4))

sherlock_rdd.saveAsTextFile("output")
sc.stop()