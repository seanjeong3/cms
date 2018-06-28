import Adafruit_GPIO.I2C as I2C
import time
import datetime


MPUADDR = 0x68
BUSNUM = 2
PWR_MGMT_1 = 0x6B
ACC_X = 0x3B
ACC_Y = 0x3D
ACC_Z = 0x3F
GYR_X = 0x43
GYR_Y = 0x45
GYR_Z = 0x47
ACC_SF = 16384.0
GYR_SF = 131.0



def start_daq(time_step,num_sample):
	# create an instance of a sensor connected to i2c
	sensor = I2C.Device(MPUADDR, BUSNUM)
	# wake up the sensor
	sensor.write8(PWR_MGMT_1, 0)
	
	ax_list = []
	ay_list = []
	az_list = []
	dt_list = []

	for i in range(num_sample):
		ax_list.append(sensor.readS16(ACC_X, False) / ACC_SF)
		ay_list.append(sensor.readS16(ACC_Y, False) / ACC_SF)
		az_list.append(sensor.readS16(ACC_Z, False) / ACC_SF)
		dt_list.append(datetime.datetime.now())
		time.sleep(time_step)

	return dt_list,ax_list,ay_list,az_list