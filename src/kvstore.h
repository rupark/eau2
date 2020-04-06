//
// Created by Kate Rupar on 3/21/20.
//

#pragma once
#include "key.h"
#include <iostream>
#include <unistd.h>
#include <cstdlib>

using namespace std;

class DataFrame;

/**
 * Represents a Key Value Store
 */
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

    /**
     * Adds the given Key and DataFrame to this KVStore
     */
    void put(Key* key, DataFrame* df) {
        cout << "put " << size << endl;
        this->keys[size] = key;
        cout << "df null: " << (df == nullptr) << endl;
        this->dfs[size] = df;

        size++;
    }

    /**
     * Returns the DataFrame associated in this KVStore with the given Key
     */
    DataFrame* get(Key key) {
        cout << "size: " << size << endl;
        for (int i = 0; i < size; i++) {
            if (key.equals(keys[i])) {
                cout << "key found: " << i << endl;
                cout << dfs[i]->ncol << endl;
                if (dfs[i] != nullptr) {
                    return dfs[i];
                } else {
                    cout << "asked for nullptr df" << endl;
                }
            }
        }
        cout << "key not found" << endl;
        return nullptr;
    }

    //Client facing
    // Wait until we find the key (TIMEOUT?)
    DataFrame* waitAndGet(Key key) {
        if (key.home==0) {
            cout << "wait triggering get" << endl;
            return get(key);
        } else {
            cout << "ERROR: NOT WORKING WAIT AND GET NETWORK" << endl;
        }
//
//        cout << "wait and get" << endl;
//        while (get(key) == nullptr) {
//            //send a message requesting
//            key.home;
//            sleep(3);
//        }
//        this->get(key);
    }

};
