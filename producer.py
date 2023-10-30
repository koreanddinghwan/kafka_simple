import socket, sys, signal

from server import makeKafkaMsg

global server

def main(argv, args):
    if (argv.__len__() != 3 or argv[2].isdigit() == False):
        print("Usage: python3 producer.py <server_ip> <server_port>")
        return

    #connect to server
    global server
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.connect((argv[1], int(argv[2])))
    print('Connected to server')

    #get input from user
    while True:
        msg = input('>>')
        kafkaMsg = makeKafkaMsg(msg, 'EVENT')
        print(str(msg.__len__()) + ' events are created')
        server.send(kafkaMsg.encode())

def signal_handler(sig, frame):
    print('\nExit')
    server.shutdown(socket.SHUT_RDWR)
    server.close()
    sys.exit(0)

if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    main(sys.argv, sys.argv)
