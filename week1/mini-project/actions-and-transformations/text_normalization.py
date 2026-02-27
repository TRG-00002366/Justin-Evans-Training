'''
 This is Task 1 of the mini project
'''


from pyspark import SparkContext
import re


sc = SparkContext("local[*]", "Text Normalization")
#punction_lookup = ["'",'"','.','?','!','-',';',':',',']
broadcasted_list = sc.broadcast(punction_lookup)


rdd = sc.textFile("../sherlock-homes-book.txt")


#This function gets rid of all punction by using regular expressions
def remove_punctuation(line):
    return re.sub(r"[^\s\w]", "", line)

book_in_lowercase = rdd \
    .map(lambda line: line.lower()) \
    .map(remove_punctuation)

print(book_in_lowercase.take(5))



sc.stop()