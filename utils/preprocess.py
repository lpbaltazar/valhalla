import warnings
warnings.filterwarnings("ignore")

import os
import sys
import time

import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from utils.utils import readCSV, readCSVAsArray, get_rows, drop_rows, toCSV


def clean_pagetitle(df):
	df.loc[df.pagetitle.isnull() & ~df.readarticle.isnull(), "pagetitle"] = df.readarticle

	return df


def generateID(df):

	print("generating IDs")
	pagetitles = pd.DataFrame(df.pagetitle.unique(), columns=["pagetitle"])
	pagetitles = pagetitles.reset_index(drop=False)
	pagetitles.columns = ["iteratorid", "pagetitle"]
	
	df = df.merge(pagetitles, on="pagetitle")
	df.loc[:, "ID"] = df.bigdatasessionid.map(str) + "_" + df.iteratorid.map(str)

	# print(len(df))
	# print(len(df.drop_duplicates()))

	df[["bigdatasessionid", "pagetitle", "ID"]].to_csv("../data/id.csv", index=False)


def preprocess(filename):

	s = time.time()

	cols = readCSVAsArray("../data/colnames.csv")
	actiontaken = readCSVAsArray("../data/actiontaken.csv")
	
	data = readCSV(filename, cols)
	# print(len(data))
	data = get_rows(data, actiontaken)
	# print(len(data))
	data = drop_rows(data, subset=["readarticle", "pagetitle", "videourl"])
	# print(len(data))
	data = drop_rows(data, subset=["bigdatasessionid"])
	# print(len(data))
	data = drop_rows(data, subset=["bigdatacookieid"])
	# print(len(data))
	data = drop_rows(data, subset=["fingerprintid"])
	# print(len(data))

	data = clean_pagetitle(data)
	# print(len(data))
	data_directory = "../data/preprocessed-data/"
	if not os.path.isdir(data_directory):
		os.mkdir(data_directory)

	data.to_csv(os.path.join(data_directory, filename.split("/")[-1]), index=False)

	generateID(data[["pagetitle", "bigdatasessionid"]])

	e = time.time()
	print("Runtime preprocess: ", time.strftime("%H:%M:%S", time.gmtime(e-s)), "\n")


def preprocessChunk(filename):

	s = time.time()

	cols = readCSVAsArray("../data/colnames.csv")
	timestamps = [s for s in cols if "timestamp" in s]
	actiontaken = readCSVAsArray("../data/actiontaken.csv")
	
	data = []
	chunks = pd.read_csv(filename, sep=",", usecols=cols, parse_dates=timestamps, low_memory=False, memory_map=True, chunksize = 5000000)
	for i, chunk in enumerate(chunks):
		print(i, end = "\r")
		curr = get_rows(chunk, actiontaken)
		curr = drop_rows(curr, subset=["readarticle", "pagetitle", "videourl"])
		curr = drop_rows(curr, subset=["bigdatasessionid"])
		curr = drop_rows(curr, subset=["bigdatacookieid"])
		curr = drop_rows(curr, subset=["fingerprintid"])
		curr = clean_pagetitle(curr)
		data.append(curr)

	data = pd.concat(data)
	data_directory = "../data/preprocessed-data/"
	if not os.path.isdir(data_directory):
		os.mkdir(data_directory)

	data.to_csv(os.path.join(data_directory, filename.split("/")[-1]), index=False)

	generateID(data[["pagetitle", "bigdatasessionid"]])

	e = time.time()
	print("Runtime preprocess: ", time.strftime("%H:%M:%S", time.gmtime(e-s)), "\n")


if __name__ == '__main__':
	filename = "../data/09/NewsTransactionFactTable-20190911.csv"

	preprocessed(filename)