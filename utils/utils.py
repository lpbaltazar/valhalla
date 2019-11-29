import warnings
warnings.filterwarnings("ignore")

import os
import glob
import time

import numpy as np
import pandas as pd

def readCSV(filename, cols = None):

	if cols is not None:	
		timestamps = [s for s in cols if "timestamp" in s]	
		data = pd.read_csv(filename, sep=",", usecols=cols, parse_dates=timestamps, low_memory=False, memory_map=True)

	else:
		data = pd.read_csv(filename, sep=",", low_memory=False, memory_map=True)

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


def appendCSV(data, outfile):

	if os.path.isfile(outfile):
		with open(outfile, "a+") as csv:
			data.to_csv(csv, header=False, index=False)

	else:
		data.to_csv(outfile, index=False)


def build(filename):

	s = time.time()

	source_dir = "../results/09/"
	
	filename = filename.split("/")[-1]
	filename = filename.split("-")[-1]
	filename = filename.split(".")[0]
	filename = filename.replace("2019", "")
	filename = filename[-2:]
	data_dir = os.path.join(source_dir, filename)

	filenames = glob.glob(data_dir + "/*.csv")
	data = readCSV("../data/id.csv")
	data.set_index("ID", inplace = True)


	for f in sorted(filenames):
		print(f)
		if "referral_information" in f: continue
				
		temp = pd.read_csv(f, index_col = ["ID"])

		data = data.merge(temp, how = "left", left_index = True, right_index = True)

		data.drop_duplicates(inplace = True)

	print(data.head())
	appendCSV(data.reset_index(), os.path.join(source_dir, "news_events.csv"))

	e = time.time()
	print("Runtime build: ", time.strftime("%H:%M:%S", time.gmtime(e-s)), "\n")