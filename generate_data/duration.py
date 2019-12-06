import warnings
warnings.filterwarnings("ignore")

import os
import re
import sys
import time

import datetime as dt
import numpy as np
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from utils.utils import readCSV, add_id, toCSV, readCSVAsArray

def getReadingDuration(filename):

	s = time.time()

	cols = ["bigdatasessionid", "pagetitle", "articlecontentamount", "readingduration", "viewpageduration", "pagedepth"]

	data = readCSV(filename, cols)

	data = data.groupby(["bigdatasessionid", "pagetitle"])["articlecontentamount", "readingduration", "viewpageduration", "pagedepth"].agg(['sum'])

	data.columns = data.columns.to_flat_index()

	data.columns = [col[0] for col in data.columns]

	data = add_id(data)

	#outfile = "../results/"+filename[-6:-4]+"/reading_duration.csv"
	#toCSV(data, outfile, filename)

	toCSV(data, "reading_information.csv", filename)

	e = time.time()
	print("Runtime getReadingDuration: ", time.strftime("%H:%M:%S", time.gmtime(e-s)), "\n")


def getDateInfo(data):
	data.loc[:, "session_year"] = data.sessionstarttimestamp.dt.year 
	data.loc[:, "session_month"] = data.sessionstarttimestamp.dt.month 
	data.loc[:, "week"] = data.sessionstarttimestamp.apply(lambda x: (x + dt.timedelta(days=1)).week)
	data.loc[:, "session_day"] = data.sessionstarttimestamp.dt.day
	data.loc[:, "session_hour"] = data.sessionstarttimestamp.dt.hour 
	data.loc[:, "session_weekday"] = data.sessionstarttimestamp.apply(lambda x: (x + dt.timedelta(days=1)).dayofweek)

	return data 


def getSessionDuration(filename):
	s = time.time()

	usecols = ["bigdatasessionid", "pagetitle", "sessionstarttimestamp", "sessionendtimestamp"]
	data = readCSV(filename, usecols)
	# print(len(data))
	data = data.drop(data.loc[data.sessionendtimestamp=="NaN/NaN/NaN NaN:NaN:NaN.NaN"].index)
	# print(len(data))
	data.sessionendtimestamp = data.sessionendtimestamp.astype("datetime64[ns]")  

	data = data \
		.groupby(["bigdatasessionid", "pagetitle"])["sessionstarttimestamp", "sessionendtimestamp"] \
		.agg({"sessionstarttimestamp": "min", "sessionendtimestamp": "max"}) \
		.pipe(pd.DataFrame) \
		.reset_index()

	data.loc[:, "session_duration"] = (data.sessionendtimestamp - data.sessionstarttimestamp)/np.timedelta64(1,'s')
	data = getDateInfo(data)
	data = add_id(data)

	toCSV(data, "session_information.csv", filename)

	e = time.time()
	print("Runtime getSessionDuration: ", time.strftime("%H:%M:%S", time.gmtime(e-s)), "\n")
