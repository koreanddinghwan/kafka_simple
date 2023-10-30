import socket, sys, time, signal

from server import print_with_lock

def parseKafkaMsg(msg):
    msg = msg.split(',')
    msgType = msg[0].split('=')[1]
    data = msg[1].split('=')[1]
    return msgType, data

def main(argv, args):
    if (argv.__len__() != 3 or argv[2].isdigit() == False):
        print("Usage: python3 consumer.py <server_ip> <server_port>")
        return

    #connect to server
    global server
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    server.connect((argv[1], int(argv[2])))
    
    consumer_number = server.recv(1);

    print('Connected, Consumer ' + str(consumer_number.decode()))

    while True:
        data = server.recv(1024).decode()
        msgType, data = parseKafkaMsg(data)
        if (msgType == 'EVENT'):
            print('Event ' + str(data) + ' is processed in consumer ' + str(consumer_number.decode()))
        else:
            print('No event in Queue')
        time.sleep(1)

def signal_handler(sig, frame):
    print('Exiting consumer...')
    server.shutdown(socket.SHUT_RDWR)
    server.close()
    sys.exit(0)

if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    main(sys.argv, sys.argv)
