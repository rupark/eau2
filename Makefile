clean:
	rm *.o client server client2 *.out wordcount *.h.gch src/*.h.gch src/network/*.h.gch

build:
	g++ -std=c++11 -c tests/m4/main.cpp -o main.o
	g++ -std=c++11 src/network/wordcount.h main.o -o wordcount

run: build
	./wordcount -index 0 -file data/WCData.txt -node 1 -port 8080 -masterip "127.0.0.4" -app "wc"
