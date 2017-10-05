from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals

import time
import json
from threading import Thread
from kafka import KafkaConsumer, KafkaProducer


class BasicClient(Thread):
    daemon = True

    def __init__(self, config):
        Thread.__init__(self)
        self.config = config

        # hippo
        self.hippo_name = config['hippo']['name']
        self.sub_topics = config['hippo']['subscribe_topics'].split(',')
        self.pub_topic = config['hippo']['publish_topic']
        
        # kafka
        self.kafka_host = config['kafka']['bootstrap_servers']
        self.producer = KafkaProducer(bootstrap_servers=self.kafka_host)

    def start_consumer(self):
        self.consumer = KafkaConsumer(bootstrap_servers=self.kafka_host,
                                       auto_offset_reset='latest',
                                       group_id=self.hippo_name)
        self.consumer.subscribe(self.sub_topics)

    def should_submit(self, js_value, message):
        raise NotImplementedError
    
    def parse_value(self, js_value):
        return js_value
    
    def call_job_on_system(self, command):
        raise NotImplementedError

    def run(self):
        while True:
            self.start_consumer()
            for message in self.consumer:
                try:
                    js_value = json.loads(message.value)
                    if self.should_submit(js_value, message):
                        command = self.parse_value(js_value)
                        self.consumer.close()
                        self.call_job_on_system(command)
                        break
                except Exception as e:
                    print('Error:')
                    print(e)