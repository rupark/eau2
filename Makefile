clean:
	rm *.o client server client2 *.out wordcount linus *.h.gch src/*.h.gch src/network/*.h.gch wordcountC wordcountS eau2

buildl:
	g++ -std=c++11 -c tests/m4/main.cpp -o main.o
	g++ -std=c++11 src/network/wordcount.h main.o -o linus

buildwc:
	g++ -std=c++11 -c tests/m4/main.cpp -o main.o
	g++ -std=c++11 src/network/wordcount.h main.o -o wordcount

runwcs:
	./wordcount -index 0 -file data/100k.txt -node 3 -port 8080 -masterip "127.0.0.4" -app "wc" -rowsperchunk 10 -masterport 8080

runwcc:
	./wordcount -index 1 -file data/100k.txt -node 3 -port 8080 -masterip "127.0.0.4" -app "wc" -rowsperchunk 10 -masterport 8080

runwcc2:
	./wordcount -index 2 -file data/100k.txt -node 3 -port 8080 -masterip "127.0.0.4" -app "wc" -rowsperchunk 10 -masterport 8080

runl: buildl
	./linus -index 0 -file data/WCData.txt -node 1 -port 8080 -masterip "127.0.0.4" -app "linus"

run:
	./eau2 -index 0 -file data/100k.txt -node 1 -port 8080 -masterip "127.0.0.4" -app "wc" -rowsperchunk 10 -masterport 8080

build:
	g++ -std=c++11 -c tests/m4/main.cpp -o main.o
	g++ -std=c++11 src/network/wordcount.h main.o -o eau2

valgrind:
	valgrind --leak-check=full ./linus -index 0 -node 1 -port 8080 -masterip "127.0.0.4" -app "linus"
