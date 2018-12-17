from kafka import KafkaConsumer, KafkaProducer
from multiprocessing import process

from streamprocessor import StreamProcessors
from constants import VERSA_ANALYTICS_TOPIC, VERSA_ANALYTICS_HOST
import sys


if __name__ == '__main__':
	stream = StreamProcessors()

	if len(sys.argv) >=2:	

		if sys.argv[1] == "consumer":
			'''consume data'''
			print("[+] kafka consumer running")
			consumer_config = {}
			consumer_config["bootstrap_servers"] = VERSA_ANALYTICS_HOST
			stream.consumer("versa-analytics", bootstrap_servers=VERSA_ANALYTICS_HOST)

		elif sys.argv[1] == "producer":
			'''produce data'''
			print("[+] kafka producer running")
			message = bytes(input(">"), encoding='utf-8')
			_producer = stream.producer(bootstrap_servers=['localhost:9092'])
			stream.sendmessage(_producer, "versa-analytics", message)


	else:
		print("[-] provide valid inputs")

