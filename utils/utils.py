import warnings
warnings.filterwarnings("ignore")

import os
import time

import numpy as np
import pandas as pd

def readCSV(filename, cols = None):

	if cols is not None:
		cols = list(cols)
		data = pd.read_csv(filename, sep=",", usecols=cols, nrows = 10000)
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
		with open(outfile, "a") as csv:
			data.to_csv(csv, header=False, index=False)

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

	return data