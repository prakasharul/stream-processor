from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

from multiprocessing import process

from streamprocessor import StreamProcessors
from constants import VERSA_ANALYTICS_TOPIC, VERSA_ANALYTICS_HOST, VERSA_MONITERING_HOST, LOG
import sys
import json

if __name__ == '__main__':
	stream = StreamProcessors()

	if len(sys.argv) >=2:	

		if sys.argv[1] == "consumer":
			'''consume data from versa-analytics topic'''
			print("[+] kafka consumer running")
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
							filtered_message = json.dumps(filtered_data).encode("utf-8")
							_producer_moniter = stream.producer(bootstrap_servers=['localhost:9092'])
							if isinstance(_producer_moniter, KafkaProducer):
								send = stream.sendmessage(_producer_moniter, "versa-monitering", filtered_message)
								if send:	
									print("[+] send filtered_data to versa-monitering topic")
				except (KafkaError, Exception) as e:
					print("[-] error")
					print(str(e))


		elif sys.argv[1] == "producer":
			'''produce data to versa-anaytics topic'''
			print("[+] kafka producer running")
			message = bytes(LOG, encoding='utf-8')
			_producer = stream.producer(bootstrap_servers=['localhost:9092'])
			if isinstance(_producer, KafkaProducer):
				stream.sendmessage(_producer, "versa-analytics", message)


		elif sys.argv[1] == "monitor":
			'''consume message from versa monitoering topic '''
			print("[+] kafka consumer running")
			consumer_config = {}
			consumer_config["bootstrap_servers"] = VERSA_MONITERING_HOST
			consume = stream.consumer("versa-monitering", consumer_config)	
			for msg in consume:
				print(msg.value)

	else:
		print("[-] provide valid inputs")

