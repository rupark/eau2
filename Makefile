clean:
	rm *.o client server client2 *.out wordcount *.h.gch src/*.h.gch src/network/*.h.gch

run: build
	./server && ./client && ./client2

valgrind: 
	valgrind --leak-check=full -v ./a.out

build: client2.o client.o server.o src/object.h
	g++ -std=c++11 src/network/network.h client.o -o client
	g++ -std=c++11 src/network/network.h client2.o -o client2
	g++ -std=c++11 src/network/network.h server.o -o server

client.o:
	g++ -std=c++11 -c tests/m3/client.cpp -o client.o

client2.o:
	g++ -std=c++11 -c tests/m3/client2.cpp -o client2.o

server.o:
	g++ -std=c++11 -c tests/m3/server.cpp -o server.o

buildWC:
	g++ -std=c++11 -c tests/m4/main.cpp -o main.o
	g++ -std=c++11 src/network/wordcount.h main.o -o wordcount

runWCserver:
	./wordcount -index 0 -file data/WCData.txt -node 1 -port 8080 -masterip "127.0.0.4" -app "wc"
