# kafka_simple

- simple kafka application with producer, consumer but blocking
- use python3

<br>

# Intro

kafka is madeof JAVA, using NIO(non-blocking i/o)  

but in this project, to simplify arch, do not use kernel event notification mechanism like kqueue, select, epoll, iocp.  

<br>

## Consist

this project consist of 
1. producer
2. consumer
3. server. 

the producer and consumer communicate with server by socket.
