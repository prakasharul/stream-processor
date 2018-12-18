from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import sys, json, urllib
from pymongo import MongoClient, errors as PymongoErrors
from bson.objectid import ObjectId


from streamprocessor import StreamProcessors
from constants import VERSA_ANALYTICS_TOPIC, VERSA_ANALYTICS_HOST, VERSA_MONITERING_HOST, LOG, MONGO_DB_NAME, MONGO_DB_HOST
from logger import logger



def setup():
	''' mongodb connect function'''
	mongo_db_user = ""
	mongo_db_pwd = ""
	mongo_db_host = MONGO_DB_HOST
	mondo_db = MONGO_DB_NAME

	if mongo_db_user and mongo_db_pwd:
		uri = "mongodb://%s:%s@%s/%s" % (mongo_db_user, urllib.parse.quote(mongo_db_pwd) , mongo_db_host, mondo_db)
	else:
		uri = "mongodb://%s" % mongo_db_host

	try:
		client = MongoClient(uri, connect=False)
		db = client[mondo_db]
		if db:
			print("[+] Database connect Successfull")
	except (PymongoErrors.ConnectionFailure, PymongoErrors.ServerSelectionTimeoutError) as ex:
		print("[-] Database connect unsuccessfull")
		print(str(ex))
		logger.exception(str(ex))
	return db	

def find_device(db, data):
	print("[+] find a value from db:" +data.get("device_name"))
	find = db.devices.find_one({
		"deviceName":str(data['device_name'])
		})
	return find

def update_device(db, data):
	print("[+] update devicefrom db->device:" +data.get("deviceName"))
	update = db.devices.update({
            "deviceName":str(data["deviceName"])
        }, {
            "$set": {
                "deviceStatus": "in_service",
                "deviceLiveStatus":"up"
            }
        })
	return update

if __name__ == '__main__':
	stream = StreamProcessors()

	if len(sys.argv) >=2:	


		if sys.argv[1] == "producer":
			'''produce log data to versa-analytics topic'''
			print("[+] kafka producer running")
			message = bytes(LOG, encoding='utf-8')
			_producer = stream.producer(bootstrap_servers=['localhost:9092'])
			if isinstance(_producer, KafkaProducer):
				stream.sendmessage(_producer, "versa-analytics", message)
				print("[-] kafka producer sent data to versa-analytics")


		elif sys.argv[1] == "analytics":
			'''
			1. consume data from versa-analytics topic
			2. filter the data based on device name and event
			3. convert to json and 
			4. send to versa monitoering topic
			'''
			print("[+] kafka consumer running: versa-analytics")
			consumer_config = {}
			consumer_config["bootstrap_servers"] = VERSA_ANALYTICS_HOST
			consume = stream.consumer("versa-analytics", consumer_config)
			filtered_data ={}
			for msg in consume:
				try:
					json_data = stream.kafka_json(msg)

					if "alarmType" in json_data and json_data["alarmType"] == "sdwan-branch-disconnect":
						filtered_data["device_name"] = json_data.get("alarmKey")
						filtered_data["event_type"] = json_data.get("alarmType")

						if filtered_data:
							print("[+] message parsed to json")
							filtered_message = json.dumps(filtered_data).encode("utf-8")
							_producer_moniter = stream.producer(bootstrap_servers=['localhost:9092'])
							if isinstance(_producer_moniter, KafkaProducer):
								send = stream.sendmessage(_producer_moniter, "versa-monitering", filtered_message)
								if send:	
									print("[+] parsed message sent to versa-monitering")
				except (KafkaError, Exception) as e:
					print("[-] send data to versa-monitering failed")
					print(str(e))


		elif sys.argv[1] == "monitor":
			'''
			1. consume data from versa monitering topic 
			2. get the device name blonging recorrd from db
			3. and update device live satus and device status
			4. then sent to versa internam tpoic
			'''
			print("[+] kafka consumer for versa-monitering")
			db = setup()	
			consumer_config = {}
			consumer_config["bootstrap_servers"] = VERSA_ANALYTICS_HOST
			consume = stream.consumer("versa-monitering", consumer_config)
			filtered_data ={}
			for msg in consume:
				try:
					json_data = json.loads(msg.value.decode('utf-8'))
					if json_data:
						device = find_device(db, json_data)
						update = update_device(db, device)
						print("[+] device update is done")


						filtered_message = json.dumps(update).encode("utf-8")
						_producer_moniter = stream.producer(bootstrap_servers=['localhost:9092'])
						if isinstance(_producer_moniter, KafkaProducer):
							send = stream.sendmessage(_producer_moniter, "versa-internal", filtered_message)
							if send:	
								print("[+] send data to versa-internal topic")
				except (KafkaError, Exception) as e:
					logger.exception(str(e))
					print("[-] error")
					print(str(e))
	

		elif sys.argv[1] == "internal":
			'''consume message from versa monitering topic '''
			print("[+] kafka consumer running")
			consumer_config = {}
			consumer_config["bootstrap_servers"] = VERSA_MONITERING_HOST
			consume = stream.consumer("versa-internal", consumer_config)	
			for msg in consume:
				print(msg.value)

	else:
		print("[-] provide valid inputs")

