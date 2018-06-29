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
BROKER_ADDR = 'eil-computenode1.stanford.edu'
BROKER_PORT = 8080
USER = 'swjeong3'
PASSWORD = 'nopassword'
SUB_TOPIC = 'machine/sensor/+/out/#'

DIR_PRC = './dir1_processed_data'
DIR_RAW_REG = './dir2_regular_raw_data'
DIR_RAW_ABN = './dir3_anomaly_raw_data'

STATUS_LIST = ['IDLE', 'CHECKING_STATUS', 'UPLOADING']
SENSOR_STATUS_LIST = ['UNKNOWN', 'IDLE', 'SENSING', 'UPLOADING']

# Global variables
Connected = False
Status = STATUS_LIST[0]
Sensors = ['ACC_001', 'ACC_002', 'ACC_003', 'ACC_004']
Sensors_status = {}
Latest_sensing_time = datetime.datetime.now()

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



	# 	# Check sensor status
	# 	if topic_last == 'init':
	# 		payload = {'msg': 'CHECK_STATUS'}
	# 		client.publish('machine/sensor/in', json.dumps(payload))
	# 		Status = STATUS_LIST[1]

	# elif Status == STATUS_LIST[1]:

	# 	if topic_last == 'status':
	# 		sensor_id = msg['sensor_id']
	# 		status = msg['status']
	# 		if sensor_id in Sensor_state_list.keys():
	# 			Sensor_state_list[sensor_id] = status
	# 			Sensor_count -= 1
	# 		if Sensor_count == 0:
	# 			Sensor_count = len(Sensor_state_list)
	# 			print Sensor_state_list



	# 	if Status == STATUS_LIST[0]:
	# 		# Check sensor
	# 		if msg['msg'] == 'CHECK_STATUS':
	# 			payload = {'sensor_id': SENSOR_ID, 'status': Status}
	# 			client.publish('machine/sensor/{0}/out/status'.format(SENSOR_ID), json.dumps(payload))

	# 		# Do sensing
	# 		elif msg['msg'] == 'DO_SENSING':
	# 			Status = STATUS_LIST[1]
	# 			# event_time = msg['event_time']
	# 			# time_step = msg['time_step']
	# 			# num_sample = msg['num_sample']
	# 			t = threading.Thread(target=do_sensing)
	# 			t.daemon = True
	# 			t.start()

	# 		# Upload raw data (regular)
	# 		elif msg['msg'] == 'UPLOAD_RAWDATA_REGULAR':
	# 			Status = STATUS_LIST[2]
	# 			# event_time = msg['event_time']
	# 			t = threading.Thread(target=do_uploading_regular)
	# 			t.daemon = True
	# 			t.start()

	# 		elif msg['msg'] == 'UPLOAD_RAWDATA_ABNORMAL':
	# 			Status = STATUS_LIST[2]
	# 			# event_time = msg['event_time']
	# 			t = threading.Thread(target=do_uploading_abnormal)
	# 			t.daemon = True
	# 			t.start()

	# 	elif Status == STATUS_LIST[1]:
	# 		# Check sensor
	# 		if msg['msg'] == 'CHECK_STATUS':
	# 			payload = {'sensor_id': SENSOR_ID, 'status': Status}
	# 			client.publish('machine/sensor/{0}/out/status'.format(SENSOR_ID), json.dumps(payload))

	# 	elif Status == STATUS_LIST[2]:
	# 		# Check sensor
	# 		if msg['msg'] == 'CHECK_STATUS':
	# 			payload = {'sensor_id': SENSOR_ID, 'status': Status}
	# 			client.publish('machine/sensor/{0}/out/status'.format(SENSOR_ID), json.dumps(payload))

	# 		# # # Param update
	# 		# # # on message: (topic: machine/sensor/#/in, message: SEND_RAWDATA_ABNORMAL)
	# 		# # #             -> publish rawcdata (topic: machine/sensor/sensorID/out/raw_data_abnormal, message JSON{id, data})
	# except:
	# 	print "on_message: error"





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
			if all_checked:
				Latest_sensing_time = datetime.datetime.now()
				payload = {'msg': 'DO_SENSING',
							'event_time': Latest_sensing_time,
							'time_step': 1,
							'num_sample': 5}
				client.publish('machine/sensor/in', json.dumps(payload))
				Status = STATUS_LIST[2]

		elif Status == STATUS_LIST[2]:
			print wait
			
				








		# Check status of sensor, get the list of up-sensors
		# Let them start sensing
		# Wait for all sensors send responses
		# If all sensor send the response
		# Parse and store data
		# Do some analysis

		# If timer, get raw data
		# If anomaly, get abnormal data

		time.sleep(1)



except KeyboardInterrupt:
	print "exiting"
	client.disconnect()
	client.loop_stop()




