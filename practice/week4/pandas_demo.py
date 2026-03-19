import pandas as pd
import numpy as np

import re


data = {
    "Name": ["John", "Deep", "Julia", "Kate", "Sandy"],
    "MonthSales": [25, 30, 35, 40, 45],
    "Region": ["North", "South", "East", "West", "North"]
}

data2 = {
    "Name": ["John", "Falala", "Julia", "Kate", "Sandy"],
    "MonthSales": [31, 30, 35, 40, 45],
    "Region": ["North", "South", "East", "West", "North"]
}


df1 = pd.DataFrame(data)


df2 = pd.DataFrame(data2)


print(df1[df1["MonthSales"] > 30])

print(df1[df1["Name"] > "J"])


df_merged = df1.merge(df2, "outer")

df_merged = df_merged.drop_duplicates(subset=["Name", "Region"])

#df_drop_reg = df_merged.drop(columns=["Region"])
df_drop_reg = df_merged.iloc[2]


#df3= pd.read_csv("")
string = "This is a test string"

if re.match(r"^This", string):
    print(f"'This' is the first word in the string")
if re.search(r"string$", string):
    print(f"'This' is the first word in the string")

print(df_merged)

print(df_drop_reg)



list1 = [1,2,3,4,5,6,7,8]

str_list = list(map(lambda x: str(x), list1))
print(", ".join(str_list))

string1 = "I want to tokenize this string"
token_str = []

for char in string1:
    token_str.append(chr(ord(char)))


tokenized_str = string1.split()
print(token_str)
print("".join(token_str))
print("".join(list(map(lambda ch: ch.upper(), token_str))))

#index() returns index where something first occurs
#remove() removes the first instance of a value
#insert() insert value at a certain index
# append()
# clear()
# extend() append a list onto another list
# pop() remove an item from a specified position
#
#
#
#Str methods
# isalpha()
# isnumeric()
# isalnum()
# sub()
# chr()
# ord()
# split()
# join()
#
#dict
#values()
#keys()
# from_keys()
# 
#
#
#
#
#
#
#
#

