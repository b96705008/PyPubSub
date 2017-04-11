from __future__ import print_function
import time
from transitions import Machine
from BasicSubmitter import BasicSubmitter


class FSMSubmitter(BasicSubmitter):
	states = ['idle', 'waiting', 'submitting']

	def __init__(self, hippo_name, need_tables):
		BasicSubmitter.__init__(self, hippo_name)

		self.need_tables = need_tables
		self.curr_tables = set({})
		self.machine = Machine(model=self, states=type(self).states, initial='idle')

		# idle -> waiting
		self.machine.add_transition('add_table', '*', 'waiting', conditions=['shoud_wait'])
		self.machine.add_transition('add_table', '*', 'submitting', conditions=['is_ready'], after='submit_job')
		self.machine.add_transition('finish', '*', 'idle', after='refresh')

	def shoud_wait(self):
		return len(self.need_tables - self.curr_tables) > 0

	def is_ready(self):
		return len(self.need_tables - self.curr_tables) == 0

	def refresh(self):
		self.curr_tables = set()

	def receive_table(self, table):
		self.curr_tables.add(table)
		self.add_table()

	def print_status(self):
		print("state: {}, received_tables: {}".format(self.state, self.curr_tables))

	# == over write ===
	def submit_job(self):
		print("Start submit spark job...")
		self.call_job_on_system()
		self.finish()

	def run(self):
		for message in self.consumer:
			print(message)
			t = message.value
			self.receive_table(t)
			self.print_status()
