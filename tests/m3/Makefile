build: main2.o
	g++ -std=c++11 ../../src/network/serial.h ../../src/dataframe.h main2.o

main2.o:
	g++ -std=c++11 -c serializedf.cpp -o main2.o

clean:
	rm *.o *.out

run:
	./a.out

cbr: clean build run
	echo "clean build run"