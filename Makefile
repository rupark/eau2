clean:
	rm *.o client server client2 *.out wordcount linus *.h.gch src/*.h.gch src/network/*.h.gch wordcountC wordcountS

buildl:
	g++ -std=c++11 -c tests/m4/main.cpp -o main.o
	g++ -std=c++11 src/network/wordcount.h main.o -o linus

buildwcs:
	g++ -std=c++11 -c tests/m4/main.cpp -o main.o
	g++ -std=c++11 src/network/wordcount.h main.o -o wordcountS

buildwcc:
	g++ -std=c++11 -c tests/m4/main.cpp -o main.o
	g++ -std=c++11 src/network/wordcount.h main.o -o wordcountC

runwcs: buildwcs
	./wordcountS -index 0 -file data/WCData.txt -node 2 -port 8080 -masterip "127.0.0.4" -app "wc" -rowsperchunk 5

runwcc: 
	./wordcountC -index 1 -file data/WCData.txt -node 2 -port 8080 -masterip "127.0.0.4" -app "wc" -rowsperchunk 5

runl: buildl
	./linus -index 0 -file data/WCData.txt -node 1 -port 8080 -masterip "127.0.0.1" -app "linus"
