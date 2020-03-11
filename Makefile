build:  bool.h boolcol.h column.h dataframe.h fielder.h float.h floatcol.h helper.h intcol.h integer.h list.h object.h row.h rower.h schema.h sorer.h string.h stringcol.h tests.o
	clang++ --std=c++11 bool.h boolcol.h column.h dataframe.h fielder.h float.h floatcol.h helper.h intcol.h integer.h list.h object.h row.h rower.h schema.h sorer.h string.h stringcol.h tests.o

tests.o:
	clang++ --std=c++11 -c main.cpp -o tests.o

clean:


run:
	./a.out -f data_big.txt -from 0 -len 10000
