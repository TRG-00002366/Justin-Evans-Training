from pyspark import SparkContext
import re

sc = SparkContext("local[*]", "Sentence Collector")

book_rdd = sc.textFile("../sherlock-homes-book.txt")




def remove_punctuation(line):
    return re.sub(r"[^\s\w]", "", line)




#T4 Stuff

print("-"*(60))
print("Task 4")
letter_counter = sc.accumulator(0)

def letter_count(line):
    letter_counter.add(len(line))
    return line

list_of_characters = book_rdd.map(letter_count)

result = list_of_characters.collect()

print(f"Number of letters in book: {letter_counter.value}")
print("-"*(60))
#End of T4

book_header_ending_index = 27

def data_filter(row):
    #print(row[1])
    return True

indexed_rdd = book_rdd.filter(lambda line: len(line) > 0) \
    .zipWithIndex() \
    .filter(lambda line: line[1] > book_header_ending_index and line[1] < 10088)




unindexed = indexed_rdd.map(lambda line: line[0])

unindexed_rdd = sc.parallelize([unindexed.reduce(lambda a, b: a + " " + b)])





# T5 Stuff

print("Task 5")

def break_into_sentences(line):

    line = line.replace("Mr.", "Mr")
    line = line.replace("Mrs.", "mrs")
    line = line.split(".")


    return line


def fix_mr_and_mrs(line):
    line = line.replace("Mr", "Mr.")
    line = line.replace("mrs", "Mrs.")
    
    

    return line

broken_by_periods = unindexed_rdd \
    .flatMap(break_into_sentences) \
    .flatMap(lambda line: line.split("?")) \
    .flatMap(lambda line: line.split("!")) \
    .map(fix_mr_and_mrs) 
    

sentence_lengths = broken_by_periods \
    .map(lambda line: (line, len(line))) \
    .sortBy(lambda line: line[1], ascending=False)

print(f"Longest sentence: {sentence_lengths.take(1)}")

print("-"*(60))
#End of T5 Stuff


#T6 Stuff

print("Task 6")

watson_sentences = broken_by_periods \
    .map(lambda line: line.replace('"', '')) \
    .filter(lambda line: "Watson" in line)

print("First 10 Sentences containing Watson")
print(watson_sentences.take(10))
#print(watson_sentences.collect())

print("-"*(60))

#End of T6



#Start of T7
print("Task 7, 8, and 10")


unique_word_counter = sc.accumulator(0)
total_word_size = sc.accumulator(0)


def count_unique_words(word):
    unique_word_counter.add(1)
    total_word_size.add(len(word[0]))
    return word



unique_words = broken_by_periods \
    .map(lambda line: line.lower()) \
    .map(remove_punctuation) \
    .flatMap(lambda line: line.split()) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda word1, word2: word1 + word2) \
    .map(count_unique_words) \
    .sortBy(lambda word: word[1], ascending=False)

result = unique_words.collect()
print(f"Top 10 words: {unique_words.take(10)}")
print(f"Number of unique words: {unique_word_counter.value}")
print(f"Avg word length: {total_word_size.value / unique_word_counter.value}")



def retrieve_first_word(line):
    return line.split()[0]










print("-"*(60))


print("Task 9")

unique_first_word_counter = sc.accumulator(0)

def count_first_words(word):
    unique_first_word_counter.add(1)
    return word

first_words_of_lines = broken_by_periods \
    .map(lambda line: line.lower()) \
    .map(remove_punctuation) \
    .filter(lambda line: len(line) > 0) \
    .map(retrieve_first_word) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda word1, word2: word1 + word2) \
    .sortBy(lambda word: word[1], ascending=False)

print(f"Top 10 most common line starters: {first_words_of_lines.take(10)}")


print("-"*(60))



#End of task 9




#Start of task 11


word_size_tallies = broken_by_periods \
    .map(lambda line: line.lower()) \
    .map(remove_punctuation) \
    .flatMap(lambda line: line.split()) \
    .map(lambda word: (len(word), 1)) \
    .reduceByKey(lambda word1, word2: word1 + word2) \
    .sortBy(lambda word: word[1], ascending=False)


print(f"Word length Tallies: {word_size_tallies.collect()}")

#End of task 11


print("-"*(60))


#Start of task 12





print("-"*(60))
# Start of O3
print("Task O3")

words_with_length = broken_by_periods \
    .map(lambda line: line.lower()) \
    .map(remove_punctuation) \
    .flatMap(lambda line: line.split()) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda word1, word2: word1 + word2) \
    .map(lambda word: (len(word[0]), word[0])) \
    .sortBy(lambda word: word[0], ascending=False)

print(f"Words Paired with Their Length: {words_with_length.take(100)}")

print("-"*(60))
# End of O3 


#Start of O4

char_frequency = broken_by_periods \
    .map(lambda line: line.lower()) \
    .map(remove_punctuation) \
    .flatMap(lambda line: line.split()) 
    # .map(lambda letter: (letter, 1)) \
    # .reduceByKey(lambda letter1, letter2: letter1 + letter2) \
    # .sortBy(lambda letter: letter[0], ascending=False)

print(f"Character Frequencies: {char_frequency.collect()}")