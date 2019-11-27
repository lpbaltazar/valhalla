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

def getReadingDuration(filename):

	s = time.time()

	cols = ["bigdatasessionid", "pagetitle", "articlecontentamount", "readingduration", "viewpageduration", "pagedepth"]

	data = readCSV(filename, cols)

	data = data.groupby(["bigdatasessionid", "pagetitle"])["articlecontentamount", "readingduration", "viewpageduration", "pagedepth"].agg(['max', 'sum'])

	data.columns = data.columns.to_flat_index()

	data.columns = ['_'.join(col) for col in data.columns]

	data = add_id(data)

	outfile = "../results/"+filename[-6:-4]+"/reading_duration.csv"
	
	toCSV(data, outfile)

	e = time.time()
	print("Runtime getReadingDuration: ", time.strftime("%H:%M:%S", time.gmtime(e-s)))


def getDateInfo(data):
	data.loc[:, "session_year"] = data.sessionstarttimestamp.dt.year 
	data.loc[:, "session_month"] = data.sessionstarttimestamp.dt.month 
	data.loc[:, "week"] = data.sessionstarttimestamp.apply(lambda x: (x + dt.timedelta(days=1)).week)
	data.loc[:, "session_day"] = data.sessionstarttimestamp.dt.day
	data.loc[:, "session_hour"] = data.sessionstarttimestamp.dt.hour 
	data.loc[:, "session_weekday"] = data.sessionstarttimestamp.apply(lambda x: (x + dt.timedelta(days=1)).dayofweek)

	return data 


def getSessionDuration(filename):
	start_time = time.time()

	usecols = ["bigdatasessionid", "pagetitle", "sessionstarttimestamp", "sessionendtimestamp"]
	data = readCSV(filename, usecols)

	data = data \
		.groupby(["bigdatasessionid", "pagetitle"])["sessionstarttimestamp", "sessionendtimestamp"] \
		.agg({"sessionstarttimestamp": "min", "sessionendtimestamp": "max"}) \
		.pipe(pd.DataFrame) \
		.reset_index()

	data = getDateInfo(data)
	data = add_id(data)

	toCSV(data, "../results/09/session_information.csv")

	print("getSessionDuration RUNTIME: ", time.time() - start_time)


if __name__ == '__main__':
	
	filename = "../data/preprocessed-data/NewsTransactionFactTable-20190911.csv"

	getReadingDuration(filename)