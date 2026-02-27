'''
 This is Task 5 of the mini project    
'''


from pyspark import SparkContext

sc = SparkContext("local[*]", "Length Analysis")


rdd = sc.textFile("../sherlock-homes-book.txt")



print(rdd.take(100))