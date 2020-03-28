//
// Created by kramt on 3/25/2020.
//
#include "network.h"

int main(int argc, char* argv[]) {

    NetworkIP* client = new NetworkIP();
    client->client_init(1, 8080, "127.0.0.1", 8080);

}