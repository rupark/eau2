//
// Created by kramt on 3/25/2020.
//
#include "network.h"

int main(int argc, char* argv[]) {

    NetworkIP* server = new NetworkIP();
    server.server_init(0, 8080);

}