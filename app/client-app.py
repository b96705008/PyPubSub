from __future__ import print_function
import os
import subprocess
from threading import Thread
from kafka import KafkaConsumer
import SimpleHTTPServer
import SocketServer

APP_DIR = os.path.dirname(os.path.realpath(__file__))
PORT = 8000

class Consumer(Thread):
	daemon = True

	def __init__(self, topics):
		Thread.__init__(self)
		self.topics = topics

	def submit_job(self):
		code = subprocess.call([
			"spark-submit",
			"{}/spark-app.py".format(APP_DIR)
		])
		return code

	def run(self):
		self.consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
								 auto_offset_reset='latest',
								 group_id='test_group')
		self.consumer.subscribe(self.topics)

		for message in self.consumer:
			print(message)
			v = message.value
			if v == "submit":
				code = self.submit_job()
				print("submit spark job result: {}".format(code))


if __name__ == '__main__':
	print("Start kafka consumer...")
	c = Consumer(['test'])
	c.start()

	# == start a server ==
	#input("Wait for message...")
	Handler = SimpleHTTPServer.SimpleHTTPRequestHandler
	httpd = SocketServer.TCPServer(("", PORT), Handler)
	print("serving client app at port: {}".format(PORT))
	httpd.serve_forever()
