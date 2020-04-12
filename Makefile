clean:
	rm *.o client server client2 *.out wordcount linus *.h.gch src/*.h.gch src/network/*.h.gch wordcountC wordcountS

buildl:
	g++ -std=c++11 -c tests/m4/main.cpp -o main.o
	g++ -std=c++11 src/network/wordcount.h main.o -o linus

buildwc:
	g++ -std=c++11 -c tests/m4/main.cpp -o main.o
	g++ -std=c++11 src/network/wordcount.h main.o -o wordcount

runwcs:
	./wordcount -index 0 -file data/WCData.txt -node 2 -port 8080 -masterip "127.0.0.3" -app "wc" -rowsperchunk 5 -masterport 8080

runwcc:
	./wordcount -index 1 -file data/WCData.txt -node 2 -port 8080 -masterip "127.0.0.3" -app "wc" -rowsperchunk 5 -masterport 8080

runl: buildl
	./linus -index 0 -file data/WCData.txt -node 1 -port 8080 -masterip "127.0.0.1" -app "linus"
