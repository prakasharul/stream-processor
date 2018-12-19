# stream processor kafka-python

###### python class for kafka-python with producer, consumer and send message feature.

### dependency 

```sh
pip install -r requirement.txt
```

### usage

```python
from streamprocessor import StreamProcessors

stream = StreamProcessors()
#producer
producer = stream.producer(bootstrap_servers=['localhost:9092'])
#send data to topic
stream.sendmessage(_producer, "your-topic", "your-message")
#consumer
consumer = stream.consumer("your-topic", "config-dict")

```



