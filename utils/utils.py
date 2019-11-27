import warnings
warnings.filterwarnings("ignore")

import os
import time

import numpy as np
import pandas as pd

def readCSV(filename, cols):

	cols = list(cols)

	if not cols:
		data = pd.read_csv(filename, sep=",")

	else:
		data = pd.read_csv(filename, sep=",", usecols=cols)

	return data


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

	data.dropna(subset = subset, inplace = True)

	return data