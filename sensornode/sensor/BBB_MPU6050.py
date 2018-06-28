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
CAL_NUM = 100
TS = 2


if __name__ == '__main__':
	# create an instance of a sensor connected to i2c
	sensor = I2C.Device(MPUADDR, BUSNUM)
	# wake up the sensor
	sensor.write8(PWR_MGMT_1, 0)

	print "Start calibration..."
	sumAx = 0
	sumAy = 0
	sumAz = 0
	sumGx = 0
	sumGy = 0
	sumGz = 0
	for i in range(CAL_NUM):
		sumAx += sensor.readS16(ACC_X, False) / ACC_SF
		sumAy += sensor.readS16(ACC_Y, False) / ACC_SF
		sumAz += sensor.readS16(ACC_Z, False) / ACC_SF
		sumGx += sensor.readS16(GYR_X, False) / GYR_SF
		sumGy += sensor.readS16(GYR_Y, False) / GYR_SF
		sumGz += sensor.readS16(GYR_Z, False) / GYR_SF
	calAx = sumAx / CAL_NUM
	calAy = sumAy / CAL_NUM
	calAz = sumAz / CAL_NUM - 1.0
	calGx = sumGx / CAL_NUM
	calGy = sumGy / CAL_NUM
	calGz = sumGz / CAL_NUM	
	print "End calibration!"


	while True:
		ax = round(sensor.readS16(ACC_X, False) / ACC_SF - calAx, 3)
		ay = round(sensor.readS16(ACC_Y, False) / ACC_SF - calAy, 3)
		az = round(sensor.readS16(ACC_Z, False) / ACC_SF - calAz, 3)
		gx = round(sensor.readS16(GYR_X, False) / GYR_SF - calGx, 3)
		gy = round(sensor.readS16(GYR_Y, False) / GYR_SF - calGy, 3)
		gz = round(sensor.readS16(GYR_Z, False) / GYR_SF - calAz, 3)

		print "{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}".format(datetime.datetime.now(), ax, ay, az, gx, gy, gz)
		time.sleep(TS)





