import warnings
warnings.filterwarnings("ignore")

import os
import re
import sys
import time

import numpy as np
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from utils.utils import readCSV, add_id, toCSV


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

	data["referraltype"] = data[["currentwebpage", "previouswebpage"]].apply(lambda x: get_referraltype(x), axis = 1)

	data = add_id(data)

	outfile = "../results/"+filename[-6:-4]+"/referral_information.csv"
	
	toCSV(data, outfile)

	e = time.time()
	print("Runtime getReferralInformation: ", time.strftime("%H:%M:%S", time.gmtime(e-s)))


if __name__ == '__main__':
	
	filename = "../data/preprocessed-data/NewsTransactionFactTable-20190911.csv"

	getReferralInformation(filename)