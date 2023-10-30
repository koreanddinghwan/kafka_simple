import socket, sys, time, signal

def main(argv, args):
    if (argv.__len__() != 3):
        print("Usage: python3 consumer.py <server_ip> <server_port>")
        return

    #connect to server
    global server
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.connect((argv[1], int(argv[2])))
    
    consumer_number = server.recv(8);

    print('Connected, Consumer ' + str(consumer_number.decode()))

    while True:
        data = server.recv(1024)
        #Event 1 is processed in consumer 1
        if (data):
            print('Event ' + str(data.decode()) + ' is processed in consumer ' + str(consumer_number.decode()))
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
