//
// Created by Kate Rupar on 4/5/20.
//

#include "../../src/args.h"
#include "../../src/network/network.h"
#include "../../src/network/thread.h"
#include "../../src/applications/application.h"
#include "../../src/applications/wordcount.h"
#include <iostream>
#include <stdio.h>
#include "../../src/CS4500NE/parser.h"
#include "../../src/dataframe/dataframe.h"
#include "../../src/applications/linus.h"
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

int main(int argc, char *argv[]) {
    arg.parse(argc, argv);

    NetworkIP *network = initialize();
    assert(arg.num_nodes != 0 && "cannot have empty cloud");

    if (strcmp(arg.app, "wc") == 0) {
        WordCount *app = new WordCount(network->index(), *network);
        cout << "CHOSEN APP: " << arg.app << endl;
        app->run_();
        cout << "Finished Running App" << endl;
        delete app;
    } else {
        Linus *app = new Linus(network->index(), *network);
        cout << "CHOSEN APP: " << arg.app << endl;
        app->run_();
        cout << "Finished Running App" << endl;
        delete app;
    }
}

