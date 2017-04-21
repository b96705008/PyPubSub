from __future__ import print_function
import os
import subprocess
from threading import Thread
from kafka import KafkaConsumer, KafkaProducer

SCRIPT = '/Users/roger19890107/Developer/main/projects/cathay/hippo/PyPubSub/scripts/submit.sh'

class BasicSubmitter(Thread):
	daemon = True

	def __init__(self, hippo_name):
		Thread.__init__(self)
		self.hippo_name = hippo_name
		self.sub_topics = ['test']
		self.pub_topic = 'test'

		# consumer
		self.consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
								 	  auto_offset_reset='latest',
								 	  group_id=hippo_name)
		self.consumer.subscribe(self.sub_topics)

		# producer
		self.producer = KafkaProducer(bootstrap_servers='localhost:9092')

	def pub_job_result(self, code):
		pub_msg = b"{} finish spark-job with code: {}".format(self.hippo_name, code)
		self.producer.send(self.pub_topic, pub_msg)

	def call_job_on_system(self):
		code = subprocess.call([
			'/bin/sh',
			SCRIPT
		])
		print("submit spark job result: {}".format(code))
		self.pub_job_result(code)

	def submit_job(self):
		print("Start submit spark job...")
		self.call_job_on_system()

	def run(self):
		for message in self.consumer:
			print(message)
			v = message.value
			if v == "submit":
				self.submit_job()
