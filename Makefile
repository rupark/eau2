clean:
	rm *.out *.o *.gch.h

build: dataframe.h key.h kvstore.h main2.o
	g++ -std=c++11 dataframe.h key.h kvstore.h main2.o

main2.o:
	g++ -std=c++11 -c main2.cpp -o main2.o

run:
	./a.out
