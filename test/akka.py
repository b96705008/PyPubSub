# https://github.com/jodal/pykka
from __future__ import print_function
import pykka


class Greeter(pykka.ThreadingActor):
    daemon=True
    def on_receive(self, message):
        print('Hi there!')


if __name__ == '__main__':
    actor_ref = Greeter().start()
    actor_ref.tell({'msg': 'Hi?'})
    actor_ref.stop()
