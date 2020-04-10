clean:
	rm *.o client server client2 *.out wordcount linus *.h.gch src/*.h.gch src/network/*.h.gch

buildl:
	g++ -std=c++11 -c tests/m4/main.cpp -o main.o
	g++ -std=c++11 src/network/wordcount.h main.o -o linus

buildwc:
	g++ -std=c++11 -c tests/m4/main.cpp -o main.o
	g++ -std=c++11 src/network/wordcount.h main.o -o wordcount 

runwc: buildwc
	./wordcount -index 0 -file data/WCData.txt -node 1 -port 8080 -masterip "127.0.0.4" -app "wc"

runl: buildl
	./wordcount -index 0 -file data/WCData.txt -node 1 -port 8080 -masterip "127.0.0.4" -app "linus"
