import paho.mqtt.client as mqtt
import json
import numpy as np
import dateutil.parser 
import datetime as dt
import StringIO
from PIL import Image


class MQTT_Handler:

	def __init__(self, host, port='8080', qos=1, user_id=None, password=None):
		self.host = host
		self.port = port
		self.qos = qos
		self.user_id = user_id
		self.passwor = password		
		self.client = mqtt.Client('gateway_cloud_interface')
		self.client.connect(self.host, port=self.port)		
		self.client.loop_start()

	def publish_processed_data(self, data):
		infot = self.client.publish('machine/gateway/out/processed_data', data, qos=self.qos)
		infot.wait_for_publish()

	def publish_raw_data_regular(self, data):
		infot = self.client.publish('machine/gateway/out/raw_data_regular', data, qos=self.qos)
		infot.wait_for_publish()

	def publish_raw_data_abnormal(self, data):
		infot = self.client.publish('machine/gateway/out/raw_data_abnormal', data, qos=self.qos)
		infot.wait_for_publish()

