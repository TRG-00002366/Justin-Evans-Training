'''
 This is Task 4 of the mini project
'''

from pyspark import SparkContext

sc = SparkContext("local[*]", "Character Counting")

letter_count = sc.accumulator(0)



rdd = sc.textFile("../sherlock-homes-book.txt")



def letter_counter(line):
    letter_count.add(len(line))
    return line

list_of_characters = rdd.map(letter_counter)

result = list_of_characters.collect()

print(f"Number of letters in book: {letter_count.value}")