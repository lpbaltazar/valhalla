import warnings
warnings.filterwarnings("ignore")

import os
import re
import sys
import time

import numpy as np
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from utils.utils import readCSV, add_id, toCSV, readCSVAsArray


def get_referraltype(x):

	if pd.isnull(x[1]):
		return "DIRECT"
	elif check(x[1], "social"):
		return "SOCIAL"
	elif search("news.abs-cbn.com", x[0]) & search("news.abs-cbn.com", x[1]):
		return "INTERNAL"
	elif check(x[1], "search_engine"):
		return "SEARCH"
	else:
		return "EXTERNAL"


def search(pattern, string):

	if re.search(pattern, string):
		return True
	else:
		return False	


def check(string, check_type):

	if check_type == "social":
		base = ["facebook.com", "twitter.com", "instagram.com", "fb.com", "youtube.com", "tumblr.com"]
	elif check_type == "search_engine":
		base = ["google.com", "bing.com", "yahoo.com", "baidu.com", "ask.com", "aol.com", "duckduckgo.com", "msn.com"]
	else:
		print("unaccaptable check type")


	for url in base:
		if search(url, string):
			return True

	return False


def getReferralInformation(filename):

	s = time.time()

	cols = ["bigdatasessionid", "pagetitle", "currentwebpage", "previouswebpage"]
	
	data = readCSV(filename, cols = cols)

	print(len(data))

	data.dropna(subset = ["currentwebpage"], inplace = True)

	print(len(data))

	data["referraltype"] = data[["currentwebpage", "previouswebpage"]].apply(lambda x: get_referraltype(x), axis = 1)

	data = add_id(data)

	print(len(data))
#
	data.dropna(subset = ["ID"], inplace = True)

	print(len(data))
	
	toCSV(data, "referral_information.csv", filename)

	e = time.time()
	print("Runtime getReferralInformation: ", time.strftime("%H:%M:%S", time.gmtime(e-s)), "\n")


def getInformation(filename, mode):
	s = time.time()

	if mode == "content":
		usecols = readCSVAsArray("../data/content_information.csv")
		drop_cols = ["pagetitle", "videourl"]
	else:
		usecols =readCSVAsArray("../data/device_information.csv")
		drop_cols = usecols[:-1]

	data = readCSV(filename, cols=usecols)
	# data = data.drop_duplicates(subset=drop_cols)
	data = add_id(data)

	toCSV(data, mode + "_information.csv", filename)

	e = time.time()
	print("Runtime getInformation_", mode, ": ", time.strftime("%H:%M:%S", time.gmtime(e-s)), "\n")


def getUserInformation(filename):
	s = time.time()

	usecols = ["bigdatasessionid", "pagetitle", "gigyaid", "fingerprintid", "bigdatacookieid"]
	data = readCSV(filename, usecols)
	# data = data.drop_duplicates(subset=usecols[:-2])

	data.loc[:, "userid"] = data.gigyaid
	data.loc[data.userid.isnull(), "userid"] = data.fingerprintid.map(str) + "_" + data.bigdatacookieid.map(str)

	data.loc[:, "usertype"] = "NOTLOGGEDIN"
	data.loc[~data.gigyaid.isnull(), "usertype"] = "LOGGEDIN"

	data.drop_duplicates(inplace = True)

	data = add_id(data)
	
	toCSV(data, "user_information.csv", filename)

	e = time.time()
	print("Runtime getUserInformation: ", time.strftime("%H:%M:%S", time.gmtime(e-s)), "\n")
	