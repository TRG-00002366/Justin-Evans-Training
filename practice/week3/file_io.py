



# with open("./demofile.txt", "r") as file:
#     current_line = file.readline()
#     while current_line:
#         print(current_line)
#         current_line = file.readline()


# with open("./demofile.txt", "r") as file:
#     print(file.read())


# string = "This will be the contents of a new file which is pretty cool all in all"

# with open("./demofileoutput.txt", "w") as file:
#     file.write(string)



# with open("./demofileoutput.txt", "r") as file:
#     print(file.read())



import pandas as pd

dict1 = {"col1": [1,2,3,4], "col2": ["he", "hello", "its me", "last one"]}

df = pd.DataFrame(dict1)

print(df)


df_from_csv = pd.read_csv("./customers-100 (1).csv")

limited_col = df_from_csv[["First Name", "Last Name"]]

print(df_from_csv.head(10))
print(limited_col.head(10))


filtered_df = df_from_csv[(df_from_csv["Subscription Date"] > "2021-06-01") & (df_from_csv["Subscription Date"] < "2022-02-01")]
print(filtered_df)


#filter_j_names = df_from_csv[(df_from_csv["First Name"] >= "J") & (df_from_csv["First Name"] < "K")]
filter_j_names = df_from_csv[df_from_csv["First Name"].str.startswith("J")]
print(filter_j_names)


# agg_index = df_from_csv["Index"].sum()

agg_index = df_from_csv['Index'].agg(['mean', 'median', 'sum'])

print(agg_index)

df_w_domain = df_from_csv

domain_stuff = df_w_domain["Email"].str.split(".", expand=True)

df_w_domain['domain'] = domain_stuff[1]



# df_w_domain["domain"] = df_w_domain["domain"][1]

print(df_w_domain)
print(domain_stuff)




