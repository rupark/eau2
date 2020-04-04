//
// Created by kramt on 3/25/2020.
//
#pragma once
#include "../kvstore.h"
#include "network.h"

/**
 * The start of our Application class which will be started on each node of the system
 */
class Application {
public:
    KVStore* kv;
    size_t idx_;
    NetworkIP* net;

    Application (size_t idx, NetworkIP net) {
        kv = new KVStore();
        idx_ = idx;
        this->net = net;
    }

    size_t this_node() {
        return idx_;
    }

    virtual void run_() {}
};
