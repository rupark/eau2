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
        this->keys = new Key *[500];
        this->dfs = new DataFrame *[500];
    }

    ~KVStore() {
        for (int i = 0; i < size; i++) {
            delete keys[i];
            delete dfs[i];
        }
        delete[] keys;
        delete[] dfs;
    };

    /**
     * Adds the given Key and DataFrame to this KVStore
     */
    void put(Key key, DataFrame *df) {
//        if (key->name->equals(new String("users-1-0"))) {
////            cout << "??????????????????????????????????????????????????????????????????????????" << endl;
//        }

        cout << key.name->cstr_ << endl;
        assert(df != nullptr && "Putting Dataframe Null Pointer!");
        cout << "size: " << size << "new key: " << key.name->c_str() << endl;
        for (int i = 0; i < size; i++) {
            cout << this->keys[i]->name->cstr_ << endl;
        }

        cout << "h" << endl;

        // check if key is already there
        for (size_t k = 0; k < size; k++) {
            // if found already, replace
            if (key.equals(keys[k])) {
//                cout << "put key already found: " << keys[k]->name->c_str() << " at " << k << endl;
                this->dfs[k] = df;
//                cout << "DF Set new size = " << size << endl;
                return;
            }
        }

        cout << " we put" << endl;

        // if new key add and increment size
        this->keys[size] = &key;
        this->dfs[size] = df;

        cout << keys[size]->name->cstr_ << endl;

//        cout << "dfs[0]- " << dfs[0] << endl;


        size++;
//        cout << "put done new size: " << size << endl;
    }

    /**
     * Returns the DataFrame associated in this KVStore with the given Key
     */
    DataFrame *get(Key key) {
//        cout << "in get size: " << size << endl;
//        cout << "key given name:" << key.name->c_str() << endl;
        for (int i = 0; i < size; i++) {
//            cout << "keys: " << keys[i]->name->cstr_ << endl;
            if (key.equals(keys[i])) {
//                cout << "found : " << i << endl;
//                cout << "dfs[0]" << dfs[0] << endl;
                //size--;
                //delete keys[i];
                //delete dfs[i];
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
