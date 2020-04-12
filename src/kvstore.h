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
    Key **keys;
    DataFrame **dfs;
    int size;

    KVStore() {
        this->size = 0;
        this->keys = new Key *[100 * 100 * 100];
        this->dfs = new DataFrame *[100 * 100 * 100];
    }

    /**
     * Adds the given Key and DataFrame to this KVStore
     */
    void put(Key *key, DataFrame *df) {
        cout << key->name->c_str() << endl;
        cout  << "in put" << endl;
        cout << "size: " << size << endl;
        // check if key is already there
        for (size_t k = 0; k < size; k++) {
            cout << "in loop" << endl;
            // if found already, replace
            if(this->keys[k]->equals(key)) {
                cout << "found key" << endl;
                this->dfs[k] = df;
                return;
            }
        }

        cout << "new key" << endl;
        // if new key add and increment size
        this->keys[size] = key;
        this->dfs[size] = df;
        size++;
        cout << "done with put" << endl;
    }

    /**
     * Returns the DataFrame associated in this KVStore with the given Key
     */
    DataFrame *get(Key key) {
        //cout << key.name->c_str() << endl;
        cout << "get" << endl;
        for (int i = 0; i < size; i++) {
            if (key.equals(keys[i])) {
                cout << "found key" << endl;
                //if (dfs[i] != nullptr) {
                    return dfs[i];
//                } else {
//                }
            }
        }
        return nullptr;
    }

    //Client facing
    // Wait until we find the key
    DataFrame *waitAndGet(Key key) {
        return get(key);
//        if (key.home == 0) {
//            return get(key);
//        } else {
//            cout << "ERROR: NOT WORKING WAIT AND GET NETWORK" << endl;
//        }
    }

};
