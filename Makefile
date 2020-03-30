clean: cleancs
	rm src/*.out src/*.o src/*.h.gch src/network/*.gch.h client server *.o

cleancs:
	rm *.o client server client2

build: main2.o
	g++ -std=c++11 src/dataframe.h src/key.h src/kvstore.h main2.o

main2.o:
	g++ -std=c++11 -c tests/m2/main2.cpp -o main2.o

run: build
	./a.out

valgrind: 
	valgrind --leak-check=full -v ./a.out

buildcs: client2.o client.o server.o src/object.h
	g++ -std=c++11 src/network/network.h client.o -o client
	g++ -std=c++11 src/network/network.h client2.o -o client2
	g++ -std=c++11 src/network/network.h server.o -o server

client.o:
	g++ -std=c++11 -c src/network/client.cpp -o client.o

client2.o:
	g++ -std=c++11 -c src/network/client2.cpp -o client2.o

server.o:
	g++ -std=c++11 -c src/network/server.cpp -o server.o
