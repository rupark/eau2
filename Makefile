clean:
	rm *.o client server client2 *.out wordcount linus *.h.gch src/*.h.gch src/network/*.h.gch wordcountC wordcountS eau2 test

buildl:
	g++ -std=c++11 -c tests/m4/main.cpp -o main.o
	g++ -std=c++11 src/applications/linus.h main.o -o linus

buildwc:
	g++ -std=c++11 -c tests/m4/main.cpp -o main.o
	g++ -std=c++11 src/applications/wordcount.h main.o -o wordcount

runwcs:
	./wordcount -index 0 -file data/100k.txt -node 3 -port 8080 -masterip "127.0.0.4" -app "wc" -rowsperchunk 10 -masterport 8080

runwcc:
	./wordcount -index 1 -file data/100k.txt -node 3 -port 8080 -masterip "127.0.0.4" -app "wc" -rowsperchunk 10 -masterport 8080

runwcc2:
	./wordcount -index 2 -file data/100k.txt -node 3 -port 8080 -masterip "127.0.0.4" -app "wc" -rowsperchunk 10 -masterport 8080

runl: buildl
	./linus -index 0 -file data/WCData.txt -node 1 -port 8080 -masterip "127.0.0.4" -app "linus" -rowsperchunk 10000

runls:
	./eau2 -index 0 -file data/100k.txt -node 2 -port 8080 -masterip "127.0.0.4" -app "wc" -rowsperchunk 100 -masterport 8080

runlc:
	./eau2 -index 1 -file data/100k.txt -node 2 -port 8080 -masterip "127.0.0.4" -app "wc" -rowsperchunk 100 -masterport 8080

build:
	g++ -std=c++11 -c tests/m4/main.cpp -o main.o
	g++ -std=c++11 src/network/wordcount.h main.o -o eau2

valgrind:
	valgrind --leak-check=full --show-leak-kinds=all ./linus -index 0 -node 1 -port 8080 -masterip "127.0.0.4" -app "linus"

test:
	g++ -std=c++11 -c tests/tests.cpp -o main.o
	g++ -std=c++11 main.o -o test
	./test



