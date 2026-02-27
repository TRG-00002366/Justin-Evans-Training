'''
 This is Task 7 and 8 of the mini project    
'''


from pyspark import SparkContext
import re

sc = SparkContext("local[*]", "Unique Words")

unique_word_counter = sc.accumulator(0)



def remove_punctuation(line):
    return re.sub(r"[^\s\w]", "", line)

    def count_unique_words(word):
        unique_word_counter.add(1)
            return word


rdd = sc.textFile("../sherlock-homes-book.txt")

#This pipeline turns all words to lowercase, then removes punctuations, 
# then splits all words, then create a tuple with each word and then a 1
# then reduces that, combining tuples with the same word, then getting the count of unique words
#then finally sorting by count descending
unique_words = rdd \
    .map(lambda line: line.lower()) \
    .map(remove_punctuation) \
    .flatMap(lambda line: line.split()) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda word1, word2: word1 + word2) \
    .map(count_unique_words) \
    .sortBy(lambda word: word[1], ascending=False)

result = unique_words.collect()
print(f"Top 10 words: {unique_words.take(10)}")
print(f"Number of unique words: {unique_word_counter}")


def retrieve_first_word(line):
    return line.split()[0]

unique_first_word_counter = sc.accumulator(0)

def count_first_words(word):
    unique_first_word_counter.add(1)
    return word

first_words_of_lines = rdd \
    .map(lambda line: line.lower()) \
    .map(remove_punctuation) \
    .filter(lambda line: len(line) > 0) \
    .map(retrieve_first_word) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda word1, word2: word1 + word2) \
    .sortBy(lambda word: word[1], ascending=False)

print(rdd.take(10))
print(f"Top 10 most common line starters: {first_words_of_lines.take(10)}")




sc.stop()