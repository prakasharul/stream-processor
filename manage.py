from kafka import KafkaConsumer, KafkaProducer
from multiprocessing import process

from streamprocessor import StreamProcessors
from constants import VERSA_ANALYTICS_TOPIC, VERSA_ANALYTICS_HOST, VERSA_MONITERING_HOST
import sys


if __name__ == '__main__':
	stream = StreamProcessors()

	if len(sys.argv) >=2:	

		if sys.argv[1] == "consumer":
			'''consume data'''
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
							print(filtered_data)
							_producer_moniter = stream.producer(bootstrap_servers=['localhost:9092'])
							stream.sendmessage(_producer_moniter, "versa-monitering", filtered_data)	
							print("[+] send filtered_data to versa-monitoring topic")
					else:
						print("[-] event not found")	
				except Exception as e:
					print("[-] error")
					print(str(e))

				
			# print(filtered_data)

		elif sys.argv[1] == "producer":
			'''produce data'''
			print("[+] kafka producer running")
			message = bytes(input(">"), encoding='utf-8')
			_producer = stream.producer(bootstrap_servers=['localhost:9092'])
			stream.sendmessage(_producer, "versa-analytics", message)


		elif sys.argv[1] == "monitor":
			print("[+] kafka consumer running")
			consumer_config = {}
			consumer_config["bootstrap_servers"] = VERSA_MONITERING_HOST
			consume = stream.consumer("versa-monitering", consumer_config)	
			for msg in consume:
				# print(stream.kafka_json(msg))
				print(msg)


	else:
		print("[-] provide valid inputs")

