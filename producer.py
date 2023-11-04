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
    addrInfo = socket.getaddrinfo(sys.argv[1], sys.argv[2], family=socket.AF_INET, proto=socket.IPPROTO_TCP)
    server.connect((addrInfo[0][4][0], addrInfo[0][4][1]))
    print('Connected to server')

    #get input from user
    while True:
        msg = input('>>')
        if not msg:
            continue
        kafkaMsg = makeKafkaMsg(msg, 'EVENT')
        print(str(msg.__len__()) + ' events are created')
        server.send(kafkaMsg.encode())

def signal_handler(sig, frame):
    print('\nExit')
    server.close()
    sys.exit(0)

if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    main(sys.argv, sys.argv)
