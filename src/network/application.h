//
// Created by kramt on 3/25/2020.
//

#include "../kvstore.h"

class Application {
public:
    KVStore* kv_;
    size_t idx_;

    Application (size_t idx) {
        kv = new KVStore();
        idx_ = idx;
    }

    size_t this_node() {
        return idx_;
    }


    //one dataframe split across three n0des
    //key value stores cant talk to eachother
    //connect app to demo in a main that sets up server so everyone can reach
    //app has a key value store, extend networkip
    //need new message type with can send a dataframe- repurpose status\


};