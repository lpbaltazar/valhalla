import pandas as pd

data = pd.read_csv("../results/09/news_events.csv")
print("data length: ", len(data), "\n")

print(data.isnull().sum(axis = 0))