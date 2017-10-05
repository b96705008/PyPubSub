from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals

import time
import subprocess
import json

from message_client import ThreadPoolClient, BasicClient


class SubmitClient(BasicClient):

    def __init__(self, config, max_workers=1):
        super(SubmitClient, self).__init__(config)
        self.spark_script = config['spark']['bash_path']
    
    def should_submit(self, js_value, message):
        if message.topic == 'frontier-adw' and \
            js_value['message'] == 'submit':
            return True
        return False
    
    def parse_value(self, js_value):
        '''
        {"message": "submit", "value": 1000}
        '''
        command = {'num': str(js_value['value'])}
        return command
    
    def pub_job_result(self, code, num):
        pub_obj = {
            'hippo_name': self.hippo_name,
            'job_name': 'test-submit',
            'is_success': code == 0,
            'finish_time': int(time.time()),
            'num': num
        }
        pub_msg = json.dumps(pub_obj)
        self.producer.send(self.pub_topic, pub_msg)
    
    def call_job_on_system(self, command):
        print(command)
        code = subprocess.call([
            '/bin/sh',
            self.spark_script,
            command['num']
        ])
        self.pub_job_result(code, command['num'])
        