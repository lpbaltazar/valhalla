import warnings
warnings.filterwarnings("ignore")

import os
import glob

from utils.preprocess import preprocess, preprocessChunk
from utils.utils import build
from generate_data.information import getInformation, getUserInformation, getReferralInformation 
from generate_data.duration import getSessionDuration, getReadingDuration

if __name__ == '__main__':
	
	data_dir = "../data/09/"
	filenames = glob.glob(data_dir + "*.csv")

	for f in sorted(filenames):
		print(f)

		'''
		PREPROCESS DATA

		Output:
			../data/preprocessed-data/<filename>
		'''
		
		# preprocess(f)
		preprocessChunk(f)

		f = f.replace("09/", "preprocessed-data/")

		'''
		BUILD INFORMATION

		Builds:
			1. content information
			2. device information
			3. user information
			4. referral information

		Output
			1. ../results/09/<day>/content_information.csv
			2. ../results/09/<day>/device_information.csv
			3. ../results/09/<day>/user_information.csv
			4. ../results/09/<day>/referral_information.csv
		'''

		getInformation(f, mode = "content")
		getInformation(f, mode = "device")
		getUserInformation(f)
		getReferralInformation(f)


		'''
		BUILD DURATION

		Builds:
			1. session duration
			2. reading duration

		Output
			1. ../results/09/<day>/session_information.csv
			2. ../results/09/<day>/reading_information
		'''

		getSessionDuration(f)
		getReadingDuration(f)

		'''
		Combine all builds

		Output:
			../results/09/news_events.csv
		'''

		# build(f)