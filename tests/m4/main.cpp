//
// Created by Kate Rupar on 4/5/20.
//

#include "../../src/args.h"
#include "../../src/network/network.h"
#include "../../src/network/thread.h"
#include "../../src/network/application.h"
//#include "../../src/network/wordcount.h"
#include <iostream>
#include <stdio.h>
#include "../../src/parser.h"
#include "../../src/dataframe.h"
#include "../../src/network/linus.h"
#include <string.h>

using namespace std;

Args arg;

NetworkIP *initialize() {
    NetworkIP *res = new NetworkIP();
    if (arg.index == 0) {
        cout << "initializing server" << endl;
        res->server_init(arg.index, arg.port, arg.master_ip);
        cout << "server initialized" << endl;
    } else {
        StrBuff *client_adr = new StrBuff();
        client_adr->c("127.0.0.");
        client_adr->c(arg.index + 4);
        res->client_init(arg.index, arg.port, arg.master_ip, arg.master_port, client_adr->get()->c_str());
        delete client_adr;
    }
    return res;
}

Application *pick(size_t i, NetworkIP &net) {
    if (strcmp(arg.app, "wc") == 0) {
//        return new WordCount(i, net);
        return nullptr;
    } else if (strcmp(arg.app, "linus") == 0){
        return new Linus(i, net);
    }
}

int main(int argc, char *argv[]) {
    arg.parse(argc, argv);

    NetworkIP *network = initialize();
    assert(arg.num_nodes != 0 && "cannot have empty cloud");
//    try {
        Application *app = pick(network->index(), *network);
        cout << "CHOSEN APP: " << arg.app << endl;
        app->run_();
        cout << "Finished Running App" << endl;
        delete app;
        delete network;
//    } catch (std::exception const &e) {
//        cout << "error: " << e.what() << endl;
//    }
   // delete network;
}

