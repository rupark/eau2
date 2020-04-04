//
// Created by kramt on 3/25/2020.
//
#pragma once
#include "../kvstore.h"

/**
 * The start of our Application class which will be started on each node of the system
 */
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

    virtual run_() {}
};
