CC = gcc
OFED_PATH = /usr
DEFAULT_CFLAGS = -I${OFED_PATH}/include
DEFAULT_LDFLAGS = -L${OFED_PATH}/lib64 -L${OFED_PATH}/lib

CFLAGS += $(DEFAULT_CFLAGS) -g -O2
LDFLAGS += $(DEFAULT_LDFLAGS) -libverbs
OBJECTS = client.o server.o sock.o
TARGETS = client server

all: $(TARGETS)

client: client.o sock.o
	$(CC) $^ -o $@ $(LDFLAGS)

client.o: client.c sock.h
	$(CC) -c $(CFLAGS) $<

server: server.o sock.o
	$(CC) $^ -o $@ $(LDFLAGS)

server.o: server.c sock.h
	$(CC) -c $(CFLAGS) $<

sock.o: sock.c sock.h
	$(CC) -c $(CFLAGS) $<

clean:
	rm -f $(OBJECTS) $(TARGETS)
