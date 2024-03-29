import warnings
warnings.filterwarnings("ignore")

import os
import sys
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
	
	# data = data.drop(["bigdatasessionid", "pagetitle"], axis=1)

	data.drop_duplicates(inplace = True)
	
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

	# data = readCSV(filename, ["bigdatasessionid", "pagetitle"])
	# generateID(data)
	
	filename = filename.split("/")[-1]
	filename = filename.split("-")[-1]
	filename = filename.split(".")[0]
	filename = filename.replace("2019", "")
	filename = filename[-2:]
	data_dir = os.path.join(source_dir, filename)

	filenames = glob.glob(data_dir + "/*.csv")
	data = readCSV("../data/id.csv")
	# print(len(data))


	temp = pd.read_csv(os.path.join(data_dir, "user_information.csv"))
	# print("user", len(temp))
	data = combine(temp, data)

	temp = pd.read_csv(os.path.join(data_dir, "device_information.csv"))
	# print("device", len(temp))
	data = combine(temp, data)

	temp = pd.read_csv(os.path.join(data_dir, "content_information.csv"))
	# print("content", len(temp))
	data = combine(temp, data)

	temp = pd.read_csv(os.path.join(data_dir, "referral_information.csv"), usecols = ["bigdatasessionid", "pagetitle", "ID", "referraltype"])
	# print("referral", len(temp))
	data = combine(temp, data)

	temp = pd.read_csv(os.path.join(data_dir, "session_information.csv"))
	# print("session", len(temp))
	data = combine(temp, data)

	data.dropna(subset = ["sessionstarttimestamp"], inplace = True)
	# print("session na", len(temp))

	temp = pd.read_csv(os.path.join(data_dir, "reading_information.csv"))
	# print("reading", len(temp))
	data = combine(temp, data)

	
	# for f in sorted(filenames):
	# 	print(f)
	
	# 	temp = pd.read_csv(f)
	# 	print(">>>>>>>>", list(temp.columns))
	# 	data = data.merge(temp, how = "left", on = ["bigdatasessionid", "pagetitle", "ID"])

		
	# 	data.drop_duplicates(inplace = True)
	# 	print(len(data))

	data.dropna(subset = ["userid"], inplace = True)
	# print(data.head())
	# print(len(data))
	appendCSV(data, os.path.join(source_dir, "news_events.csv"))

	e = time.time()
	print("Runtime build: ", time.strftime("%H:%M:%S", time.gmtime(e-s)), "\n")


def combine(temp, data):
	
	temp = drop_rows(temp, ["ID"])
	
	data = data.merge(temp, how = "left", on = ["bigdatasessionid", "pagetitle", "ID"])
	data.drop_duplicates(inplace = True)
	print("meged", len(data))

	return data