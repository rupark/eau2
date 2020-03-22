clean:
	rm src/*.out src/*.o src/*.h.gch

build: main2.o
	g++ -std=c++11 src/dataframe.h src/key.h src/kvstore.h main2.o

main2.o:
	g++ -std=c++11 -c tests/m2/main2.cpp -o main2.o

run: build
	./a.out

valgrind: 
	valgrind --leak-check=full -v ./a.out
