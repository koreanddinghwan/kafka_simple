import socket, sys, struct, time, signal
from threading import Thread, Lock
from collections import deque

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


##
# @brief this single thread will be used to accept all connections from producer process
def producer_procedure():
    print('producer_procedure')
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((sys.argv[1], int(sys.argv[2])))
    server.listen(5)
    while True:
        conn, addr = server.accept()
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
        print_with_lock('[Producer connected]')
        fd = conn.fileno()
        connected_producers[fd] = conn
        while True:
            try:
                recv_data = conn.recv(8).decode()
            except:
                print_with_lock('[Producer disconnected]')
                connected_producers.pop(fd)
                conn.close()
                break
            data = str(recv_data)
            if not data:
                break
            print_with_lock('Received: ' + str(data))
            print_with_lock('[Events created]')
            queue_lock.acquire()
            for i in range(data.__len__()):
                global_queue.append(str(data[i]))
            queue_lock.release()
            print_with_lock('[Remain events: ' + str(len(global_queue)) + ']')

##
# @brief this single thread will be used to accept all connections from consumer process
def consumer_procedure(conn, addr):
    while True:
        data = ""
        fd = conn.fileno()
        queue_lock.acquire()
        if len(global_queue) != 0:
            data = str(global_queue.popleft()).encode()
            print_with_lock('[' + 'Consumed' + data.decode() + '# Of Remain events: ' + str(len(global_queue)) + ']')
            try:
                conn.send(data)
                queue_lock.release()
            except:
                # recover event
                print_with_lock('[Consumer disconnected]')
                connected_consumers.pop(fd)
                global_queue.appendleft(data.decode())
                queue_lock.release()
                break
        else:
            #check the consumer is alive by sending heartbeat
            try:
                conn.send(''.encode())
            except:
                print_with_lock('[Consumer disconnected]')
                connected_consumers.pop(fd)
            queue_lock.release()
        time.sleep(1)

def signal_handler(sig, frame):
    print('\nExit')
    for conn in connected_producers.values():
        conn.close()
    for conn in connected_consumers.values():
        conn.close()
    raise SystemExit(sig)

def main(argv, args):

    if (argv.__len__() != 4):
        print("Usage: python3 server.py <server_ip> <producer_connection port> <consumer_connection port>")
        return

    signal.signal(signal.SIGINT, signal_handler)
    producer = Thread(target=producer_procedure)
    producer.daemon = True
    producer.start()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((sys.argv[1], int(sys.argv[3])))
    server.listen(5)
    while True:
        conn, addr = server.accept()
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
        print_with_lock('[Consumer connected]')
        connected_consumers[conn.fileno()] = conn
        conn.send(str(len(connected_consumers)).encode())
        consumer = Thread(target=consumer_procedure, args=(conn, addr))
        consumer.daemon = True
        consumer.start()
        connected_consumers[conn.fileno()] = consumer

if __name__ == '__main__':
    main(sys.argv, sys.argv)
