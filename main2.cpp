// Lang::CwC

#include <stdio.h>
#include <stdlib.h>

#include <iostream>
#include "dataframe.h"
#include "key.h"
#include "kvstore.h"

using namespace std;

/**
 * The main function.
 */
int main(int argc, char* argv[]) {
            KVStore kv = *new KVStore();

            size_t SZ = 1000*1000;
            double* vals = new double[SZ];
            double sum = 0;
            for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
            Key key("triv",0);
            DataFrame* df = DataFrame::fromArray(&key, &kv, SZ, vals);
            assert(df->get_double(0,1) == 1);
            cout << sum << endl;
            cout << "passed assert1" << endl;
            DataFrame* df2 = kv.get(key);
            cout << "got df2" << endl;
            for (size_t i = 0; i < SZ; ++i) sum -= df2->get_double(0,i);
            cout << "subtracted sum" << endl;
            assert(sum==0);
            cout << "assert passed?" << endl;
//            delete df;
//            delete df2;

            cout << "we're good" << endl;
}
