import socket, sys, struct, time, signal
from threading import Thread, Lock
from collections import deque
from enum import Enum

global_queue = deque()
queue_lock = Lock()
print_lock = Lock()
consumer_lock = Lock()
connected_producers = {}
connected_consumers = {}

# when multiple threads are prints, it will be mixed up
# so, we need to use this global lock to print one by one
def print_with_lock(msg):
    print_lock.acquire()
    print(msg)
    print_lock.release()

class messageType():
    EVENT = 'EVENT'
    HEARTBEAT = 'HEARTBEAT'

def makeKafkaMsg(data, msgType):
    return 'type=' + msgType +',data=' + data

def parseKafkaMsg(msg):
    msg = msg.split(',')
    if (msg.__len__() != 2):
        return None, None
    msgType = msg[0].split('=')[1]
    data = msg[1].split('=')[1]
    return msgType, data

##
# @brief this single thread will be used to accept all connections from producer process
def producer_procedure():
    print('producer_procedure')
    addrInfo = socket.getaddrinfo(sys.argv[1], sys.argv[2], family=socket.AF_INET, proto=socket.IPPROTO_TCP)
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((addrInfo[0][4][0] , addrInfo[0][4][1]))
    server.listen(5)
    while True:
        conn, addr = server.accept()
        print_with_lock('[Producer connected]')
        fd = conn.fileno()
        connected_producers[fd] = conn
        while True:
            recv_data = conn.recv(1024).decode()
            msgType, data = parseKafkaMsg(recv_data)
            if (msgType == 'EVENT' and data):
                print_with_lock('[Events created]')
                queue_lock.acquire()
                for i in range(len(data)):
                    global_queue.append(str(data[i]))
                queue_lock.release()
                print_with_lock('[Remain events: ' + str(len(global_queue)) + ']')
            else:
                #connection closed
                print_with_lock('[Producer disconnected]')
                connected_producers.pop(fd)
                conn.close()
                break

##
# @brief this single thread will be used to accept all connections from consumer process
def consumer_procedure(conn, addr):
    while True:
        data = ""
        fd = conn.fileno()
        queue_lock.acquire()
        if len(global_queue) != 0:
            data = str(global_queue.popleft())
            print_with_lock('[' + 'Consumed ' + data + ' |# Of Remain events: ' + str(len(global_queue)) + ']')
            try:
                msg = makeKafkaMsg(data, messageType.EVENT)
                conn.send(msg.encode())
                queue_lock.release()
            except:
                # recover event
                print_with_lock('[Consumer ' + str(fd) + ' disconnected]')
                global_queue.appendleft(data)
                consumer_lock.acquire()
                connected_consumers.pop(fd)
                print_with_lock('[' + str(len(connected_consumers)) + ' consumers online]')
                consumer_lock.release()
                queue_lock.release()
                break
        else:
            #check the consumer is alive by sending heartbeat
            try:
                msg = makeKafkaMsg('', messageType.HEARTBEAT)
                conn.send(msg.encode())
            except:
                queue_lock.release()
                print_with_lock('[Consumer ' + str(fd) + ' disconnected]')
                consumer_lock.acquire()
                connected_consumers.pop(fd)
                print_with_lock('[' + str(len(connected_consumers)) + ' consumers online]')
                consumer_lock.release()
                break
            queue_lock.release()
        time.sleep(1)

def signal_handler(sig, frame):
    print('\nExit')
    for fd in connected_producers:
        connected_producers[fd].close()
    raise SystemExit(sig)

def main(argv, args):

    if (argv.__len__() != 4 or argv[2].isdigit() == False or argv[3].isdigit() == False):
        print("Usage: python3 server.py <server_ip> <producer_connection port> <consumer_connection port>")
        return

    signal.signal(signal.SIGINT, signal_handler)
    producer = Thread(target=producer_procedure)
    producer.daemon = True
    producer.start()

    addrInfo = socket.getaddrinfo(sys.argv[1], sys.argv[3], family=socket.AF_INET, proto=socket.IPPROTO_TCP)
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((addrInfo[0][4][0] , addrInfo[0][4][1]))
    server.listen(5)
    while True:
        conn, addr = server.accept()
        print_with_lock('[Consumer ' + str(conn.fileno()) + ' connected]')
        consumer_lock.acquire()
        connected_consumers[conn.fileno()] = conn
        conn.send(str(conn.fileno()).encode())
        print_with_lock('[' + str(len(connected_consumers)) + ' consumers online]')
        consumer_lock.release()
        consumer = Thread(target=consumer_procedure, args=(conn, addr))
        consumer.daemon = True
        consumer.start()

if __name__ == '__main__':
    main(sys.argv, sys.argv)
