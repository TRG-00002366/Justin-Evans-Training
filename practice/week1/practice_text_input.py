from pyspark import SparkContext

sc = SparkContext("local[*]", "TEXTPRACTICE")


rdd = sc.textFile("text_file.txt")

words_split = rdd \
    .map(lambda text: text.split())

print(f"Words_split: {words_split.collect()}")




sc.stop()
