//
// Created by Kate Rupar on 4/5/20.
//

#include "../../src/args.h"
#include "../../src/network/network.h"
#include "../../src/network/thread.h"
#include "../../src/network/application.h"
#include "../../src/network/wordcount.h"
#include <iostream>
#include <stdio.h>
#include "../../src/parser.h"
#include "../../src/dataframe.h"
#include "../../src/network/linus.h"

using namespace std;

Args arg;

NetworkIP *initialize() {
    NetworkIP *res = new NetworkIP();
    if (arg.index == 0) {
        res->server_init(arg.index, arg.port, arg.master_ip);

// TODO: Incorporate SoR parser??
//        Constructing DataFrame from file
//        FILE* file = fopen(arg.file, "r");
//        size_t file_size = ftell(file);
//
//        SorParser parser{arg.file, 0, file_size, file_size};
//        parser.guessSchema();
//        parser.parseFile();
//        DataFrame* d = new DataFrame(parser.getColumnSet(), parser._num_columns);

    } else {
        StrBuff *client_adr = new StrBuff();
        client_adr->c("127.0.0.");
        client_adr->c(arg.index + 1);
        res->client_init(arg.index, arg.port, arg.master_ip, arg.master_port, client_adr->get()->c_str());
        delete client_adr;
    }
    return res;
}

Application *pick(size_t i, NetworkIP &net) {
    if (arg.app == "wc") {
        return new WordCount(i, net);
    } else if (arg.app == "linus"){
        return new Linus(i, net);
    }
}

int main(int argc, char *argv[]) {
    arg.parse(argc, argv);

    NetworkIP *network = initialize();
    assert(arg.num_nodes != 0 && "cannot have empty cloud");
    try {
        Application *app = pick(network->index(), *network);
        cout << "app set" << endl;
        app->run_();
        cout << "finished running app" << endl;
    } catch (std::exception const &e) {
        cout << "error: " << e.what() << endl;
    }
    //delete network;
}

