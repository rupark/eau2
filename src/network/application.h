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

};