from __future__ import print_function
import os
import subprocess
from threading import Thread
from concurrent import futures
from kafka import KafkaConsumer, KafkaProducer
import SimpleHTTPServer
import SocketServer

SCRIPT = '/Users/roger19890107/Developer/main/projects/cathay/hippo/PyPubSub/scripts/submit.sh'
PORT = 8000

class SparkJobSubmitter(Thread):
	daemon = True

	def __init__(self, hippo_name, max_workers=1):
		Thread.__init__(self)
		self.hippo_name = hippo_name
		self.sub_topics = ['test']
		self.pub_topic = 'test'
		self.executor = None
		self.max_workers = max_workers
		if max_workers > 1:
			self.executor = futures.ThreadPoolExecutor(max_workers=max_workers)

		# consumer
		self.consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
								 	  auto_offset_reset='latest',
								 	  group_id='test_group')
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
		print("======Submit async job for worker=======")
		if self.max_workers > 1:
			self.executor.submit(self.call_job_on_system)
		else:
			self.call_job_on_system()

	def run(self):
		for message in self.consumer:
			print(message)
			v = message.value
			if v == "submit":
				self.submit_job()


if __name__ == '__main__':
	print("Start kafka consumer...")
	submitter = SparkJobSubmitter("batch.etl.test", 1)
	submitter.start()

	# == start a server ==
	#input("Wait for message...")
	Handler = SimpleHTTPServer.SimpleHTTPRequestHandler
	httpd = SocketServer.TCPServer(("", PORT), Handler)
	print("serving client app at port: {}".format(PORT))
	httpd.serve_forever()
