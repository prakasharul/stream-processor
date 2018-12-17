from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from constants import VERSA_ANALYTICS_HOST
import json

class StreamProcessors():
	'''kafka stream processors'''

	def consumer(self, topic, **kwargs):
		'''kafka consumer '''
		_consumer = None
		try:
			kwargs["auto_offset_reset"] = 'latest' #latest # sma
			kwargs["enable_auto_commit"]= True
			kwargs['group_id'] = None

			_consumer = KafkaConsumer(topic, **kwargs)
			
			if _consumer:
				print("[+] kafka consumer connected sucessfully")
				for msg in _consumer:
					print(msg)
		except KafkaError as e:
			print("[-] kafka consumer failed to connect")
			print(str(e))	
		finally:
			return _consumer

	def producer(self, **kwargs):
		'''kafka producer'''
		_producer = None
		try:
			_producer = KafkaProducer(**kwargs)
			if _producer:
				print("[+] kafka producer connected sucessfully")
		except KafkaError as e:
			print("[-] kafka producer connection failed")
			print(str(e))
		finally:
			return _producer	

	
	def sendmessage(self, producer_instance, topic, message):
		'''send message to kafka topic'''
		try:
			producer_instance.send(topic, message)
			producer_instance.flush()
			print("[+] message send successfully")
			print("[+] message:" +str(message))
		except (KafkaError, Exception) as e:
			print("[-] message send failed")
			print(str(e))

