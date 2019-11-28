import warnings
warnings.filterwarnings("ignore")

import os
import time

import numpy as np
import pandas as pd

def readCSV(filename, cols = None):

	if cols is not None:
		timestamps = [s for s in cols if "timestamp" in s]
		
		if timestamps:
			data = pd.read_csv(filename, sep=",", usecols=cols, nrows = 10000, parse_dates=timestamps)
		else:
			data = pd.read_csv(filename, sep=",", usecols=cols, nrows = 10000, parse_dates=timestamps)

	else:
		data = pd.read_csv(filename, sep=",", nrows = 10000)

	return data


def readCSVAsArray(filename):
	df = pd.read_csv(filename, sep=",", header=None)
	df = df.values
	df = df.reshape((len(df),))

	return df


def toCSV(data, outfile):

	if os.path.isfile(outfile):
		data.to_csv(outfile, mode="a", index=False, header=False)

	else:
		data.to_csv(outfile, index=False)


def mergeDF(df1, df2, key):

	res = pd.merge(df1, df2, on=key)

	return res


def get_rows(data, action):

	data = data[data["actiontaken"].isin(action)]

	return data


def drop_rows(data, subset):

	data.dropna(subset = subset, how = "all", inplace = True)

	return data


def add_id(data):

	ids = readCSV("../data/id.csv")

	data = data.merge(ids, how = "left", on = ["bigdatasessionid", "pagetitle"])
	
	data = data.drop(["bigdatasessionid", "pagetitle"], axis=1)
	
	return data


def build(filename):

	data_dir = "../results/"+filename[-6:-4]

	data = readCSV("../data/id.csv")
	data.set_index("ID", inplace = True)

	print(len(data))
	for f in os.listdir(data_dir):
		if f.endswith("csv"):
			print(f)
			temp = pd.read_csv(os.path.join(data_dir, f), index_col = ["ID"])

			data = data.merge(temp, how = "left", left_index = True, right_index = True)

	print(len(data))

	data.drop_duplicates(inplace = True)

	print(len(data))

	print(data.isnull().sum(axis = 0))

if __name__ == '__main__':
	
	build("../data/preprocessed-data/NewsTransactionFactTable-20190911.csv")