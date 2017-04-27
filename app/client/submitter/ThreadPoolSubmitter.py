from __future__ import print_function
from concurrent import futures
import time
from BasicSubmitter import BasicSubmitter

class ThreadPoolSubmitter(BasicSubmitter):

	def __init__(self, hippo_name, max_workers):
		BasicSubmitter.__init__(self, hippo_name)
		self.max_workers = max_workers
		self.executor = futures.ThreadPoolExecutor(max_workers=max_workers)
		# consumer
		self.start_consumer()

	def submit_job(self):
		print("======Submit async job for worker=======")
		self.executor.submit(self.call_job_on_system)

	def run(self):
		for message in self.consumer:
			print(message)
			v = message.value
			if v == "submit":
				self.submit_job()

