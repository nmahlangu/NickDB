# NickDB
# Nicholas Mahlangu
# NickDB is a relational database.

all: clean client server

client: client.c
	gcc -O0 -g -std=c99 -Wall -Werror -lreadline client.c -o client

server: server.c
	gcc -O0 -g -std=c99 -Wall -Werror server.c -o server

clean:
	rm -f *.o a.out core client server
