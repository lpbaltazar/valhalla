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
	pagetitles = pd.DataFrame(df.pagetitle.unique(), columns=["pagetitle"])
	pagetitles = pagetitles.reset_index(drop=False)
	pagetitles.columns = ["iteratorid", "pagetitle"]
	
	df = df.merge(pagetitles, on="pagetitle")
	df.loc[:, "ID"] = df.bigdatasessionid.map(str) + "_" + df.iteratorid.map(str)

	df[["bigdatasessionid", "pagetitle", "ID"]].to_csv("../data/id.csv", index=False)


def preprocess(filename):
	cols = readCSVAsArray("../data/colnames.csv")
	actiontaken = readCSVAsArray("../data/actiontaken.csv")
	
	data = readCSV(filename, cols)
	data = get_rows(data, actiontaken)
	data = drop_rows(data, subset=["readarticle", "pagetitle", "videourl"])
	data = drop_rows(data, subset=["bigdatasessionid"])
	data = drop_rows(data, subset=["bigdatacookieid"])
	data = drop_rows(data, subset=["fingerprintid"])

	data = clean_pagetitle(data)

	data_directory = "../data/preprocessed-data/"
	if not os.path.isdir(data_directory):
		os.mkdir(data_directory)

	data.to_csv(os.path.join(data_directory, filename.split("/")[-1]), index=False)

	generateID(data[["pagetitle", "bigdatasessionid"]])