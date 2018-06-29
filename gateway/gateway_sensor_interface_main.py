# MQTT-based communication w/ gateway device

import paho.mqtt.client as mqttClient
import threading
import time
import json
import datetime
import os
import os.path
import glob

# Metadata, which may need to be stored in a separate file
GATEWAY_ID = 'GTW_001'
BROKER_ADDR = 'localhost'
BROKER_PORT = 1883
USER = 'swjeong3'
PASSWORD = 'nopassword'
SUB_TOPIC = 'machine/sensor/+/out/#'

DIR_PRC = './dir1_processed_data'
DIR_RAW_REG = './dir2_regular_raw_data'
DIR_RAW_ABN = './dir3_anomaly_raw_data'

STATUS_LIST = ['INIT', 'IDLE', 'WAIT_SENSING', 'ANALYSIS', 'WAIT_RAW_DATA_ABNORMAL', 'CHECK_REGULAR_UPLOAD', 'WAIT_RAW_DATA_REGULAR']
SENSOR_STATUS_LIST = ['UNKNOWN', 'IDLE', 'SENSING', 'UPLOADING']

SAMPLING_TS = 0.05
SAMPLING_NUM = 200
REGULAR_UPLOAD_FREQ = 10

# Global variables
Connected = False
Status = STATUS_LIST[0]
Sensors = ['ACC_001', 'ACC_002', 'ACC_003', 'ACC_004']
Sensors_status = {}
Sensors_processed_data = {}
Sensors_raw_data_abnormal = {}
Latest_sensing_time = datetime.datetime.now()
DAQ_COUNT = 0

# Callback functions
def on_connect(client, userdata, flags, rc):
	if rc == 0:
		print("Connected to broker")
		global Connected                #Use global variable
		Connected = True                #Signal connection 
	else:
		print("Connection failed")


def on_message(client, userdata, message):
	global Status
	global Sensors_status
	global Sensors_processed_data
	global Sensors_raw_data_abnormal

	# try:
	msg = json.loads(message.payload)
	topic = message.topic
	topic_last = topic.split('/')[-1]
	
	if Status == STATUS_LIST[0]:
		pass

	elif Status == STATUS_LIST[1]:
		sensor_id = msg['sensor_id']
		status = msg['status']
		if sensor_id in Sensors:
			Sensors_status[sensor_id] = status

	elif Status == STATUS_LIST[2]:
		if topic_last == 'preprocessed_data':
			sensor_id = msg['sensor_id']
			event_time = msg['event_time']
			data = msg['data']
			if sensor_id in Sensors:
				Sensors_status[sensor_id] = SENSOR_STATUS_LIST[1]
				Sensors_processed_data[sensor_id] = data

	elif Status == STATUS_LIST[3]:
		pass

	elif Status == STATUS_LIST[4]:
		if topic_last == 'raw_data_abnormal':
			sensor_id = msg['sensor_id']
			event_time = msg['event_time']
			data = msg['data']
			if sensor_id in Sensors:
				Sensors_status[sensor_id] = SENSOR_STATUS_LIST[1]
				Sensors_raw_data_abnormal[sensor_id] = data

	elif Status == STATUS_LIST[5]:
		pass

	elif Status == STATUS_LIST[6]:
		if topic_last == 'raw_data_regular':
			sensor_id = msg['sensor_id']
			event_time = msg['event_time']
			data = msg['data']
			if sensor_id in Sensors:
				Sensors_status[sensor_id] = SENSOR_STATUS_LIST[1]
				Sensors_raw_data_regular[sensor_id] = data		





# Set up data repository folder
if not os.path.exists(DIR_PRC):
    os.makedirs(DIR_PRC)
if not os.path.exists(DIR_RAW_REG):
    os.makedirs(DIR_RAW_REG)
if not os.path.exists(DIR_RAW_ABN):
    os.makedirs(DIR_RAW_ABN)


# Set up connection
client = mqttClient.Client(GATEWAY_ID)            	   #create new instance
client.username_pw_set(USER, password=PASSWORD)    #set username and password
client.on_connect= on_connect                      #attach function to callback
client.on_message= on_message                      #attach function to callback


# Connect to broker
client.connect(BROKER_ADDR, port=BROKER_PORT)
client.loop_start()       	 #start the loop
while Connected != True:   	 #Wait for connection
	time.sleep(1)


# Subscribes the designated channel
client.subscribe(SUB_TOPIC)


try:
	while True:
		if Status == STATUS_LIST[0]:
			# Iniitialize status dictionary
			Sensors_status = {}
			for sensor in Sensors:
				Sensors_status[sensor] = SENSOR_STATUS_LIST[0]

			# Send check status message to sensors
			payload = {'msg': 'CHECK_STATUS'}
			client.publish('machine/sensor/in', json.dumps(payload))
			Status = STATUS_LIST[1]


		elif Status == STATUS_LIST[1]:
			all_checked = True
			# Check sensors' response
			for key in Sensors:
				if Sensors_status[key] != SENSOR_STATUS_LIST[1]:
					all_checked = False

			# Start sensing
			if all_checked:
				Latest_sensing_time = datetime.datetime.now()
				Sensors_processed_data = {}
				payload = {'msg': 'DO_SENSING',
							'event_time': Latest_sensing_time.isoformat(),
							'time_step': SAMPLING_TS,
							'num_sample': SAMPLING_NUM}
				client.publish('machine/sensor/in', json.dumps(payload))
				for sensor in Sensors:
					Sensors_status[sensor] = SENSOR_STATUS_LIST[2]
				DAQ_COUNT += 1
				print "Start DAQ ({0}): {1}".format(DAQ_COUNT,Latest_sensing_time.isoformat())
				Status = STATUS_LIST[2]


		elif Status == STATUS_LIST[2]:
			all_finished = True
			# Check sensors' response
			for key in Sensors:
				if Sensors_status[key] != SENSOR_STATUS_LIST[1]:
					all_finished = False
			
			# Once data are acquired, 
			if all_finished:
				# store it to the designated folder				
				for sensor in Sensors:
					filename = "{0}/{1}_{2}.dat".format(DIR_PRC,sensor,Latest_sensing_time)
					f= open(filename,"w+")
					f.write(json.dumps(Sensors_processed_data[sensor]))
					f.close()
				Status = STATUS_LIST[3]


		elif Status == STATUS_LIST[3]:
			# Analyze data to check anomaly, if anomaly exists, get the raw data
			# For testing, very simple analysis...
			is_abnormal = False
			
			# DO analysis
			for sensor in Sensors:
				data = Sensors_processed_data[sensor]
				if data['ax_max'] > 0.9:
					is_abnormal = True
			
			# If abnormaly, get the data
			if is_abnormal:
				Sensors_raw_data_abnormal = {}
				payload = {'msg': 'UPLOAD_RAWDATA_ABNORMAL',
							'event_time': Latest_sensing_time.isoformat()}
				client.publish('machine/sensor/in', json.dumps(payload))
				for sensor in Sensors:
					Sensors_status[sensor] = SENSOR_STATUS_LIST[3]
				Status = STATUS_LIST[4]
			else:
				Status = STATUS_LIST[5]

		elif Status == STATUS_LIST[4]:
			all_finished = True
			# Check sensors' response
			for key in Sensors:
				if Sensors_status[key] != SENSOR_STATUS_LIST[1]:
					all_finished = False
			# Once data are acquired, 
			if all_finished:
				# store it to the designated folder				
				for sensor in Sensors:
					filename = "{0}/{1}_{2}.dat".format(DIR_RAW_ABN,sensor,Latest_sensing_time)
					f= open(filename,"w+")
					f.write(json.dumps(Sensors_raw_data_abnormal[sensor]))
					f.close()
				Status = STATUS_LIST[5]


		# check time for storing raw data
		elif Status == STATUS_LIST[5]:
			if DAQ_COUNT % REGULAR_UPLOAD_FREQ == 0:
				Sensors_raw_data_regular = {}
				payload = {'msg': 'UPLOAD_RAWDATA_REGULAR',
							'event_time': Latest_sensing_time.isoformat()}
				client.publish('machine/sensor/in', json.dumps(payload))
				for sensor in Sensors:
					Sensors_status[sensor] = SENSOR_STATUS_LIST[3]
				Status = STATUS_LIST[6]
			else:
				Status = STATUS_LIST[0]

		elif Status == STATUS_LIST[6]:
			all_finished = True
			# Check sensors' response
			for key in Sensors:
				if Sensors_status[key] != SENSOR_STATUS_LIST[1]:
					all_finished = False
			# Once data are acquired, 
			if all_finished:
				# store it to the designated folder				
				for sensor in Sensors:
					filename = "{0}/{1}_{2}.dat".format(DIR_RAW_REG,sensor,Latest_sensing_time)
					f= open(filename,"w+")
					f.write(json.dumps(Sensors_raw_data_regular[sensor]))
					f.close()
				Status = STATUS_LIST[0]

				


		
		# Parse and store data
		# Do some analysis

		# If timer, get raw data
		# If anomaly, get abnormal data

		time.sleep(0.1)



except KeyboardInterrupt:
	print "exiting"
	client.disconnect()
	client.loop_stop()




