# Without communication part
# => Check folder
# If use MQTT
# => Set up communication
from data_handler import Processed_data_handler, Raw_data_handler
from event_handler import Watcher, Handler
from mqtt_handler import MQTT_Handler

DIR1 = 'dir1_processed_data'
REPO1 = 'repo1_processed_data'
DIR2 = 'dir2_regular_raw_data'
REPO2 = 'repo2_regular_raw_data'
DIR3 = 'dir3_anomaly_raw_data'
REPO3 = 'repo3_anomaly_raw_data'

HOSTNAME = 'eil-computenode1.stanford.edu'
# URI_PROCESSED_DATA = '/sensor_data_processed'
# URI_REGULAR_DATA = '/sensor_data_raw_regular'
# URI_ABNORMAL_DATA = '/sensor_data_raw_abnormal'

if __name__ == '__main__':

	mqtt_handler = MQTT_Handler(HOSTNAME)	
	w = Watcher()
	handler1 = Processed_data_handler(DIR1,REPO1,mqtt_handler.publish_processed_data)
	w.Add_target(DIR1, handler1)
	handler2 = Raw_data_handler(DIR2,REPO2,mqtt_handler.publish_raw_data_regular)
	w.Add_target(DIR2, handler2)
	handler3 = Raw_data_handler(DIR3,REPO3,mqtt_handler.publish_raw_data_abnormal)
	w.Add_target(DIR3, handler3)
	w.Run()
	

