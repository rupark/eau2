//
// Created by Kate Rupar on 3/21/20.
//

#pragma once
#include "key.h"
#include <iostream>

using namespace std;

class DataFrame;

class KVStore {
public:
    Key** keys;
    DataFrame** dfs;
    int size;

    KVStore() {
        this->size = 0;
        this->keys = new Key*[100*100*100];
        this->dfs = new DataFrame*[100*100*100];
    }

    void put(Key* key, DataFrame* df) {
        this->keys[size] = key;
        this->dfs[size] = df;
        size++;
    }

    DataFrame* get(Key key) {
        for (int i = 0; i < size; i++) {
            if (keys[i]->equals(&key)) {
                return dfs[i];
            }
        }
        cout << "key not found" << endl;
        exit(-1);
    }

    //TODO
    DataFrame* getAndWait(Key key) {
        this->get(key);
    }

};
