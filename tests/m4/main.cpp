//
// Created by Kate Rupar on 4/5/20.
//

#include "../../src/args.h"
#include "../../src/network/network.h"
#include "../../src/network/thread.h"
#include "../../src/network/application.h"
#include "../../src/network/wordcount.h"
#include "../../src/network/demo.h.h"
#include <iostream>
using namespace std;

Args arg;

//TODO ?????
//Data* Data::

NetworkIP * initialize() {
    NetworkIP* res = new NetworkIP();
    if (arg == 0) {
        res->server_init(arg.index, arg.port);
    } else {
        res->client_init(arg.index, arg.port, arg.master_ip, arg.master_port);
    }
    return res;
}

class ApplicationThread: public Thread {
    Application* app = nullptr;
    ApplicationThread() {}
    ~ApplicationThread() {}
    void run() {
        app.startKVStore();
        app.start();
    }
};

Application* pick(size_t i, NetworkIP& net) {
    if (strcmp(arg.app, "demo") == 0) return new Demo(i, net);
    if (strcmp(arg.app, "wc") == 0) return new WordCount(i, net);
}

int main (int argc, char* argv[]) {
    arg.parse(argc,argv);
    NetworkIP* network = initialize();
    assert(arg.num_nodes != 0 && "cannot have empty cloud");
    try{
        Application* app = pick(network->index(), *network);
        app->startKVStore();
        app->start();
    } catch {
        cout << "error" << endl;
    }
    delete network;
}

