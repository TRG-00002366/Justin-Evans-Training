'''
 This is Task 6 of the mini project    
'''


from pyspark import SparkContext

sc = SparkContext("local[*]", "Word Search")


rdd = sc.textFile("../sherlock-homes-book.txt")

watson_lines = rdd.filter(lambda line: "Watson" in line)



print(watson_lines.take(100))