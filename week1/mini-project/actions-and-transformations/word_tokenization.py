'''
 This is Task 2 of the mini project
'''

from pyspark import SparkContext

sc = SparkContext("local[*]", "Text Normalization")

rdd = sc.textFile("../sherlock-homes-book.txt")


tokenized_book = rdd.flatMap(lambda line: line.split())

print(tokenized_book.take(50))