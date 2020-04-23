//
// Created by Kate Rupar on 3/21/20.
//

#pragma once

#include "../dataframe/dataframe.h"
#include "key.h"
#include <iostream>
#include <unistd.h>
#include <cstdlib>

using namespace std;

/**
 * Represents a Key Value Store
 */
class KVStore {
public:
    vector<Key*> keys;
    vector<DataFrame*> dfs;
//    Key **keys;
//    DataFrame **dfs;
//    int size;

    KVStore() {
//        this->size = 0;
//        this->keys = new Key *[500];
//        this->dfs = new DataFrame *[500];
    }

    ~KVStore() {
        cout << "in kv des" << endl;
        cout << this->keys.size() << endl;
        for (int i = 0; i < keys.size(); i++) {
                delete keys[i];
        }
        cout << "done w keys" << endl;
        for (int i = 0; i < dfs.size(); i++) {
            if (dfs[i] != nullptr) {
                delete dfs[i];
            }
        }
        cout << "done with ind" << endl;
//        delete[] keys;
//        delete[] dfs;
    };

    /**
     * Adds the given Key and DataFrame to this KVStore
     */
    void put(Key *key, DataFrame *df) {
        cout << "size of kv: " << keys.size() << "new key: " << key->name->c_str() << endl;

        // check if key is already there
        for (size_t k = 0; k < keys.size(); k++) {
            // if found already, replace
            if (this->keys[k]->equals(key)) {
                this->dfs[k] = df;
                return;
            }
        }

        // if new key add and increment size
        this->keys[size] = key;
        this->dfs[size] = df;

       // size++;
        cout << "put done new size: " << size << endl;
    }

    /**
     * Returns the DataFrame associated in this KVStore with the given Key
     */
    DataFrame *get(Key key) {
        for (int i = 0; i < keys.size(); i++) {
            if (key.equals(keys[i])) {
                return dfs[i];
            }
        }

        return nullptr;
    }

    /** Wait until we find the key **/
    DataFrame *waitAndGet(Key key) {
        return get(key);
    }

};