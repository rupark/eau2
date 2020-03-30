//
// Created by Kate Rupar on 3/21/20.
//

#pragma once
#include "key.h"
#include <iostream>
#include <unistd.h>

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
        //make client
    }

    /**
     * Adds the given Key and DataFrame to this KVStore
     */
    void put(Key* key, DataFrame* df) {
        this->keys[size] = key;
        this->dfs[size] = df;
        size++;
    }

    /**
     * Returns the DataFrame associated in this KVStore with the given Key
     */
    DataFrame* get(Key key) {
//        // check home node
//        if (key.homeNode == 0)

        for (int i = 0; i < size; i++) {
            if (keys[i]->equals(&key)) {
                return dfs[i];
            }
        }
        cout << "key not found" << endl;
        return nullptr;
    }

    //Client facing
    // Wait until we find the key (TIMEOUT?)
    DataFrame* getAndWait(Key key) {
        while (get(key) == nullptr) {
            //send a message requesting
            key.homeNode;
            sleep(3);
        }
        this->get(key);
    }

};
