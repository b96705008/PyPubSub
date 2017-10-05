from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals


from transitions import Machine
from threading import Timer
from basic import BasicClient

# Aim to collect all the required messages before submit job
class FSMClient(BasicClient):
    states = ['idle', 'waiting', 'submitting']

    def __init__(self, config, need_msgs, wait_secs=None):
        super(FSMClient, self).__init__(config)

        # fsm related
        self.need_msgs = need_msgs
        self.curr_msgs = set({})
        self.machine = Machine(model=self, states=self.states, initial='idle')

        # timeout
        self.timer = None
        self.wait_secs = wait_secs

        # idle -> waiting
        self.machine.add_transition('new_msg', '*', 'waiting', conditions=['shoud_wait'], after='refresh_timer')
        self.machine.add_transition('new_msg', '*', 'submitting', conditions=['is_ready'])
        self.machine.add_transition('finish', '*', 'idle', after='refresh')

    def stop_timer(self):
        if self.timer is not None:
            self.timer.cancel()
            self.timer = None

    def on_timeout(self):
        print('timeout!!')
        self.finish()

    def refresh_timer(self):
        self.stop_timer()
        if self.wait_secs is not None:
            self.timer = Timer(self.wait_secs, self.on_timeout)
            self.timer.start()

    def shoud_wait(self):
        return len(self.need_msgs - self.curr_msgs) > 0

    def is_ready(self):
        return len(self.need_msgs - self.curr_msgs) == 0

    def refresh(self):
        self.stop_timer()
        self.curr_msgs = set()

    def receive_msg(self, msg):
        self.curr_msgs.add(msg)
        self.new_msg()

    def print_status(self):
        print("state: {}, received_msgs: {}".format(self.state, self.curr_msgs))

    def submit_job(self):
        print("Start submit spark job...")
        self.stop_timer()
        self.call_job_on_system()
        self.finish()

    def run(self):
        while True:
            self.start_consumer()
            for message in self.consumer:
                m = message.value
                self.receive_msg(m)
                self.print_status()

                if self.state == 'submitting':
                    self.consumer.close()
                    self.submit_job()
                    break
                
