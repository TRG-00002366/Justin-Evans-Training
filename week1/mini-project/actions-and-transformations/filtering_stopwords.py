'''
 This is Task 3 of the mini project
'''

from pyspark import SparkContext
import re


sc = SparkContext("local[*]", "Text Normalization")

rdd = sc.textFile("../sherlock-homes-book.txt")

#This function is used to filter out basic words
def word_filter(line):
    line = re.sub(r"the|a|an|is")