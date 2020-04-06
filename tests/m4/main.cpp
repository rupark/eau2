//
// Created by Kate Rupar on 4/5/20.
//

#include "../../src/args.h"
#include "../../src/network/network.h"
#include "../../src/network/thread.h"
#include "../../src/network/application.h"
#include "../../src/network/wordcount.h"
//#include "../../src/network/demo.h"
#include <iostream>
#include <stdio.h>

using namespace std;

//Args arg;

//TODO ?????
//Data* Data::

NetworkIP * initialize() {
    NetworkIP* res = new NetworkIP();
    if (arg.index == 0) {
        res->server_init(arg.index, arg.port, arg.master_ip);
    } else {
        StrBuff* client_adr = new StrBuff();
        client_adr->c("127.0.0.");
        client_adr->c(arg.index+1);
        res->client_init(arg.index, arg.port, arg.master_ip, arg.master_port, client_adr->get()->c_str());
        delete client_adr;
    }
    return res;
}

class ApplicationThread: public Thread {
    Application* app = nullptr;
    ApplicationThread() {}
    ~ApplicationThread() {}
    void run() {
        app->startKVStore();
        app->start();
    }
};

Application* pick(size_t i, NetworkIP& net) {
//    if (strcmp(arg.app, "demo") == 0) return new Demo(i, net);
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
    } catch (std::exception const & e) {
        cout << "error: " << e.what() << endl;
    }
    delete network;
}

