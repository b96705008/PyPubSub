from __future__ import print_function
import time
from transitions import Machine


class FSMJobManager(object):

    states = ['idle', 'waiting', 'submitting']

    def __init__(self, need_tables):
        self.need_tables = need_tables
        self.curr_tables = set({})
        self.machine = Machine(model=self, states=FSMJobManager.states, initial='idle')

        # idle -> waiting
        self.machine.add_transition('add_table', '*', 'waiting', conditions=['shoud_still_wait'])
        self.machine.add_transition('add_table', '*', 'submitting', conditions=['is_ready'])
        self.machine.add_transition('finish', '*', 'idle', after='refresh')

    @property
    def received_tables(self):
        return self.curr_tables

    def shoud_still_wait(self):
        return len(self.need_tables - self.curr_tables) > 0

    def is_ready(self):
        return len(self.need_tables - self.curr_tables) == 0

    def refresh(self):
        self.curr_tables = set()

    def receive_table(self, table):
        self.curr_tables.add(table)
        self.add_table()

    def print_status(self):
        print("state: {}, received_tables: {}".format(self.state, self.received_tables))

if __name__ == '__main__':
    fsm = FSMJobManager({'A', 'B', 'C'})
    fsm.print_status()

    for i in range(4):
        t = chr(65 + i)
        fsm.receive_table(t)
        fsm.print_status()

    fsm.finish()
    fsm.print_status()
