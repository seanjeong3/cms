# MQTT-based communication w/ gateway device

import paho.mqtt.client as mqttClient
import threading
import time
import json
from i2c_handler import start_daq
import datetime
import os
import os.path
import glob

# Metadata, which may need to be stored in a separate file
SENSOR_ID = 'ACC_001'
BROKER_ADDR = 'eil-computenode1.stanford.edu'
BROKER_PORT = 8080
USER = 'swjeong3'
PASSWORD = 'nopassword'
SUB_TOPIC = 'machine/sensor/in'
DIR_RAW = './data_raw'
DIR_PRC = './data_prc'

STATUS_LIST = ['IDLE', 'SENSING', 'UPLOADING']

# Global variables
Connected = False
Status = STATUS_LIST[0]

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

	try:
		msg = json.loads(message.payload)
		
		if Status == STATUS_LIST[0]:
			# Check sensor
			if msg['msg'] == 'CHECK_STATUS':
				payload = {'sensor_id': SENSOR_ID, 'status': Status}
				client.publish('machine/sensor/{0}/out/status'.format(SENSOR_ID), json.dumps(payload))

			# Do sensing
			elif msg['msg'] == 'DO_SENSING':
				Status = STATUS_LIST[1]
				if 'event_time' in msg.keys(): 
					event_time = msg['event_time']
				if 'time_step' in msg.keys(): 
					time_step = msg['time_step']
				if 'num_sample' in msg.keys():
					num_sample = msg['num_sample']
				t = threading.Thread(target=do_sensing)
				t.daemon = True
				t.start()

			# Upload raw data (regular)
			elif msg['msg'] == 'UPLOAD_RAWDATA_REGULAR':
				Status = STATUS_LIST[2]
				# event_time = msg['event_time']
				t = threading.Thread(target=do_uploading_regular)
				t.daemon = True
				t.start()

			elif msg['msg'] == 'UPLOAD_RAWDATA_ABNORMAL':
				Status = STATUS_LIST[2]
				# event_time = msg['event_time']
				t = threading.Thread(target=do_uploading_abnormal)
				t.daemon = True
				t.start()

		elif Status == STATUS_LIST[1]:
			# Check sensor
			if msg['msg'] == 'CHECK_STATUS':
				payload = {'sensor_id': SENSOR_ID, 'status': Status}
				client.publish('machine/sensor/{0}/out/status'.format(SENSOR_ID), json.dumps(payload))

		elif Status == STATUS_LIST[2]:
			# Check sensor
			if msg['msg'] == 'CHECK_STATUS':
				payload = {'sensor_id': SENSOR_ID, 'status': Status}
				client.publish('machine/sensor/{0}/out/status'.format(SENSOR_ID), json.dumps(payload))

			# # # Param update
			# # # on message: (topic: machine/sensor/#/in, message: SEND_RAWDATA_ABNORMAL)
			# # #             -> publish rawcdata (topic: machine/sensor/sensorID/out/raw_data_abnormal, message JSON{id, data})
	except:
		print "on_message: error"




# Helper functions
def do_sensing(event_time=None,time_step=1,num_sample=10):
	global Status

	# DAQ
	if event_time==None: event_time = datetime.datetime.now()
	dt_list,ax_list,ay_list,az_list = start_daq(time_step,num_sample)

	# Store raw data
	filename = "{0}/{1}_{2}.dat".format(DIR_RAW,SENSOR_ID,event_time.isoformat())
	f= open(filename,"w+")
	for i in range(len(dt_list)):
		f.write("{0}\t{1}\t{2}\t{3}\n".format(dt_list[i].isoformat(),ax_list[i],ay_list[i],az_list[i]))
	f.close()
	
	# Preprocessing
	ax_min,ax_max = [min(ax_list),max(ax_list)]
	ay_min,ay_max = [min(ay_list),max(ay_list)]
	az_min,az_max = [min(az_list),max(az_list)]

	# Uploading
	payload = {'sensor_id': SENSOR_ID,
				'event_time': event_time.isoformat(),
				'data': {'ax_min':ax_min,
						'ax_max':ax_max,
						'ay_min':ay_min,
						'ay_max':ay_max,
						'az_min':az_min,
						'az_max':az_max,
						}}
	client.publish('machine/sensor/{0}/out/preprocessed_data'.format(SENSOR_ID), json.dumps(payload))
	filename = "{0}/{1}_{2}.dat".format(DIR_PRC,SENSOR_ID,event_time.isoformat())
	f= open(filename,"w+")
	for i in range(len(dt_list)):
		f.write(json.dumps(payload))
	f.close()

	# Update Status
	Status = STATUS_LIST[0]


def prepare_rawdata(event_time=None):
	# Choose file and read data
	if event_time != None:
		event_time = event_time.isoformat()
		filename = "{0}/{1}_{2}.dat".format(DIR_RAW,SENSOR_ID,event_time)
		if os.path.isfile(filename):
			f = open(filename, "r")
			data = f.readlines()
			f.close()
		else:
			payload = {'sensor_id': SENSOR_ID, 'success': 'Fail'}
			return payload
	else:
		filelist = glob.glob(DIR_RAW+'/*')
		filename = max(filelist, key=os.path.getctime)
		if os.path.isfile(filename):
			event_time = os.path.basename(filename)[8:-4]
			f = open(filename, "r")
			data = f.readlines()
			f.close()
		else:
			payload = {'sensor_id': SENSOR_ID, 'success': 'Fail'}
			return payload

	# Generate payload
	dt_list = []
	ax_list = []
	ay_list = []
	az_list = []
	for d in data:
		dt, ax, ay, az = d.split('\t')
		dt_list.append(dt)
		ax_list.append(float(ax))
		ay_list.append(float(ay))
		az_list.append(float(az))
	payload = {'sensor_id': SENSOR_ID,
				'event_time': event_time,
				'success': 'Success',
				'data': {'dt_list':dt_list,
						'ax_list':ax_list,
						'ay_list':ay_list,
						'az_list':az_list,
						}}
	return payload


def do_uploading_regular(event_time=None):
	global Status
	# Prepare payload
	payload = prepare_rawdata(event_time)
	# Upload payload
	client.publish('machine/sensor/{0}/out/raw_data_regular'.format(SENSOR_ID), json.dumps(payload))
	# Update status
	Status = STATUS_LIST[0]


def do_uploading_abnormal(event_time=None):
	global Status
	# Prepare payload
	payload = prepare_rawdata(event_time)
	# Upload payload
	client.publish('machine/sensor/{0}/out/raw_data_abnormal'.format(SENSOR_ID), json.dumps(payload))
	# Update status
	Status = STATUS_LIST[0]



# Set up data repository folder
if not os.path.exists(DIR_RAW):
    os.makedirs(DIR_RAW)
if not os.path.exists(DIR_PRC):
    os.makedirs(DIR_PRC)


# Set up connection
client = mqttClient.Client(SENSOR_ID)            	   #create new instance
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
client.subscribe('machine/sensor/{0}/in'.format(SENSOR_ID))
try:
	while True:
		time.sleep(1)
except KeyboardInterrupt:
	print "exiting"
	client.disconnect()
	client.loop_stop()




