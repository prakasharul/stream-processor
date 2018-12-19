from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import json


class StreamProcessors():
    '''kafka stream processors'''

    def consumer(self, topic, kwargs):
        '''kafka consumer '''
        _consumer = None
        try:
            kwargs["auto_offset_reset"] = 'latest' #latest # smallest
            kwargs["enable_auto_commit"]= True
            kwargs['group_id'] = None

            _consumer = KafkaConsumer(topic, **kwargs)
            
            if _consumer:
                print("[+] kafka consumer connected sucessfully")
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
        except (KafkaError,Exception) as ex:
            print("[-] message send failed")
            print(str(ex))

    
    def kafka_json(self, data):
        '''convert kafka record to json'''
        json = {}
        decoded_value = data.value.decode('utf-8')
        data_list = decoded_value.split(",")
        for item in data_list:
            try:
                key_value = item.split("=")
                json[key_value[0].strip(" ")] = key_value[1].strip(" ")
            except IndexError as e:
                print(str(e))
        return json         

