
import zmq
import pandas as pd

# Adjust Panda's output to avoid wrapping on 80 char terminal size.
pd.set_option('display.width', 200)

def price(port=5555):
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind('tcp://*:%s' % port)
    print 'Enter data:'
    while True:
        cmd = socket.recv_string()
        print cmd
        data = raw_input('>>> ')
        socket.send_string(data)


def orders(port=5556):
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect('tcp://localhost:%s' % port)
    socket.setsockopt(zmq.SUBSCRIBE, 'orders')
    while True:
        data = socket.recv()
        clear_screen()
        topic, json = data.split(' ', 1)
        df = pd.read_json(json)
        df = df.set_index(['created', 'sid', 'id'])
        df.sort_index(inplace=True)
        print df


def clear_screen():
    import os
    os.system('cls' if os.name == 'nt' else 'clear')

if __name__ == '__main__':
    # Usage: python (orders|price) [port]
    import sys

    mode = sys.argv[1]
    try:
        port = sys.argv[2]
    except IndexError:
        if mode == 'price':
            port = 5555
        else:
            port = 5556

    f = locals()[mode]
    print 'Starting: {}'.format(f.__name__)
    f(port)

