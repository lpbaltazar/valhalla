import warnings
warnings.filterwarnings("ignore")

import os
import time

import numpy as np
import pandas as pd

def readCSV(filename, cols = None):
	if cols is not None:	
		timestamps = [s for s in cols if "timestamp" in s]	
		data = pd.read_csv(filename, sep=",", usecols=cols, parse_dates=timestamps, low_memory=False, memory_map=True, nrows=100000)

	else:
		data = pd.read_csv(filename, sep=",", low_memory=False, memory_map=True, nrows=10000)

	#data = pd.concat(data)

	return data


def readCSVAsArray(filename):
	df = pd.read_csv(filename, sep=",", header=None)
	df = df.values
	df = df.reshape((len(df),))

	return df


def toCSV(data, outfile, filename):
	source_dir = "../results/09/"
	
	filename = filename.split("/")[-1]
	filename = filename.split("-")[-1]
	filename = filename.split(".")[0]
	filename = filename.replace("2019", "")
	filename = filename[-2:]
	data_dir = os.path.join(source_dir, filename)

	if not os.path.isdir(data_dir):
		os.mkdir(data_dir)

	outfile = os.path.join(data_dir, outfile)

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

if __name__ == '__main__':
	
	build("../data/preprocessed-data/NewsTransactionFactTable-20190911.csv")