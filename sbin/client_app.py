from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals

import sys
import SimpleHTTPServer
import SocketServer
import configparser

from submit_client import SubmitClient


if __name__ == '__main__':
    # load config
    conf_path = sys.argv[1]
    config = configparser.ConfigParser()
    config.read(conf_path)
    

    # kafka client
    print("Start submit client...")
    submitClient = SubmitClient(config)
    submitClient.start()

    # == start a server ==
    CLIENT_HOST = config['client']['host']
    CLIENT_PORT = int(config['client']['port'])

    Handler = SimpleHTTPServer.SimpleHTTPRequestHandler
    httpd = SocketServer.TCPServer((CLIENT_HOST, CLIENT_PORT), Handler)
    print("serving client app at port: {}".format(CLIENT_PORT))
    httpd.serve_forever()
