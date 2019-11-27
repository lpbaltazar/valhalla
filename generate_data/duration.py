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

if __name__ == '__main__':
	
	filename = "../data/preprocessed-data/NewsTransactionFactTable-20190911.csv"

	getReadingDuration(filename)