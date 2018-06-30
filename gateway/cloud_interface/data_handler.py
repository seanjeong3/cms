from glob import glob
import os
import json
import shutil
import sys
from pprint import pprint
import time
import datetime
import csv
import numpy as np

# --*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--
# CLASS: Data_handler
# DESCRIPTION: This class provide structure and basic functions to 
#              upload data to cloud
# --*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--
class Data_handler:

	def __init__(self, data_dir, repo_dir, func_upload):
		self.data_dir = data_dir
		self.repo_dir = repo_dir
		self.func_upload = func_upload
		# MQTT Listen topic
		# MQTT Listen host
		# MQTT Broadcast topic
		# MQTT Braodcast host
		# HTTP Post host
		# HTTP Post URI
		# MQTT or HTTP

	# --*--*--*--*--*--*--*--*--
	# Function: Check_data_dir
	# Description: See if new (complete) file exists in the data directory
	#  @ Input: 
	#  @ Output: List of new files
	# --*--*--*--*--*--*--*--*--
	def Check_data_dir(self):
		result = []
		# Get list of all files in the data directory
		file_list = glob(self.data_dir + "/*")
		file_size = {}
		# Check file size once
		for f in file_list:
			file_size[f] = os.path.getsize(f)
		# Check file size twice. If changed, drop from final list (i.e., result)
		for f in file_list:
			if file_size[f] == os.path.getsize(f):
				result.append(f)
		return result


	# --*--*--*--*--*--*--*--*--
	# Function: Transmit_data
	# Description: Transmit parsed data to the target server via HTTP/MQTT
	#  @Input: JSON object
	#  @ Output: Boolean weather transmission success or not
	# --*--*--*--*--*--*--*--*--
	# NOTE: Need to be updated once cloud interface is created
	def Transmit_data(self,jd):
		try:
			print("Data transmission start")
			self.func_upload(jd)
		except:
			print("Data transmission failed")
			print "Unexpected error:", sys.exc_info()[0]
			return False
		else:
			print("Data transmission success")
			return True


	# --*--*--*--*--*--*--*--*--
	# Function: Archive_data
	# Description: Backup data in a local repository
	#  @ Input: filepath
	#  @ Output: Boolean weather archiving success or not
	# --*--*--*--*--*--*--*--*--
	def Archive_data(self,f):
		try:
			new_path = "{0}/{1}".format(self.repo_dir, os.path.basename(f))
			shutil.move(f, new_path)
		except : 
			print "Unexpected error:", sys.exc_info()[0]
			return False
		else:
			return True


	# --*--*--*--*--*--*--*--*--
	# Function: Do_all
	# Description: Go through all process of this class
	#  @ Input: 
	#  @ Output: Boolean weather archiving success or not
	# --*--*--*--*--*--*--*--*--
	def Do_all(self):
		filelist = self.Check_data_dir()
		for f in filelist:
			data = self.Parse_data_file(f)
			if data == None:
				return False
			success = self.Transmit_data(data)
			if success:
				self.Archive_data(f)
			else:
				return False
		return True
		



# --*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--
# CLASS: Processed_data_handler
# DESCRIPTION: This class extends Data_handler class to handle
#              processed data
# --*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--
class Processed_data_handler(Data_handler):
	
	# --*--*--*--*--*--*--*--*--
	# Function: ParseDataFile
	# Description: Parse data file into JSON format
	#  @ Input: filepath
	#  @ Output: JSON object
	# --*--*--*--*--*--*--*--*--
	def Parse_data_file(self,f):
		# Currently very simple since the file already formatted based on JSON
		try: 
			# Mapping into JSON format
			data = json.load(open(f))
			bname = os.path.basename(f)
			sensor_id = bname.split('_')[0]
			timestamp = datetime.datetime.strptime(bname.split('_')[1][:-4], '%Y-%m-%d %H:%M:%S.%f')
			timestamp = datetime.datetime.strftime(timestamp, '%Y-%m-%dT%H:%M:%S.%f')
			json_data = {
				"sensor_id": sensor_id,
				"event_time": timestamp,
				"data": data
			}
			payload = json.dumps([json_data])
		except:
			print("Data parsing failed")
			print "Unexpected error:", sys.exc_info()[0]
			return None
		else:
			return payload



# --*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--
# CLASS: Raw_data_handler
# DESCRIPTION: This class extends Data_handler class to handle
#              raw data
# --*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--*--
class Raw_data_handler(Data_handler):
	
	# --*--*--*--*--*--*--*--*--
	# Function: ParseDataFile
	# Description: Parse data file into JSON format
	#  @ Input: filepath
	#  @ Output: JSON object
	# --*--*--*--*--*--*--*--*--
	def Parse_data_file(self,f):
		# Open file (Currently very simple since the file already formatted based on JSON)
		try:
			# Mapping into JSON format	
			with open(f, 'rb') as fs:
				data = json.load(fs)
			bname = os.path.basename(f)
			sensor_id = bname.split('_')[0]
			timestamp = datetime.datetime.strptime(bname.split('_')[1][:-4], '%Y-%m-%d %H:%M:%S.%f')
			timestamp = datetime.datetime.strftime(timestamp, '%Y-%m-%dT%H:%M:%S.%f')
			json_data = {
				"sensor_id": sensor_id,
				"event_time": timestamp,
				"data": data
			}
			payload = json.dumps([json_data])
		except:
			print("Data parsing failed")
			print "Unexpected error:", sys.exc_info()[0]
			return None
		else:
			return payload





# #--8<--8<--8<--8<--8<--8<--8<--8<- TESTING ONLY -8<--8<--8<--8<--8<--8<--8<--8<--8<--
# if __name__ == '__main__':
	
# 	DIR1 = 'dir1_processed_data'
# 	REPO1 = 'repo1_processed_data'
# 	DIR2 = 'dir2_regular_raw_data'
# 	REPO2 = 'repo2_regular_raw_data'

# 	dummy_handler = Processed_data_handler(DIR1,REPO1)
# 	filelist = dummy_handler.Check_data_dir()
# 	for f in filelist:
# 		data = dummy_handler.Parse_data_file(f)
# 		success = dummy_handler.Transmit_data(data)
# 		# if success:
# 		# 	dummy_handler.Archive_data(f)

