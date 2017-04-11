from __future__ import print_function
from concurrent import futures
from BasicSubmitter import BasicSubmitter

class ThreadPoolSubmitter(BasicSubmitter):

	def __init__(self, hippo_name, need_tables):
		BasicSubmitter.__init__(self, hippo_name)
		self.executor = None
		self.max_workers = max_workers
		if max_workers > 1:
			self.executor = futures.ThreadPoolExecutor(max_workers=max_workers)

	def submit_job(self):
		print("======Submit async job for worker=======")
		if self.max_workers > 1:
			self.executor.submit(self.call_job_on_system)
		else:
			self.call_job_on_system()
