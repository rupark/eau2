#pragma once
#include "object.h"
#include <string>
#include <iostream>
using namespace std;

class Args : public Object {
public:
    char* file = nullptr;
    bool pseudo = false;
    size_t num_nodes = 0;
    size_t rows_per_chunk = 10*1000; // how many rows per chunk
    size_t index = 0;
    size_t port = 0;
    char* master_ip;
    size_t master_port;
    char* app; // which application to run

    Args() {}

    void parse(int argc, char** argv) {
        for (int i=1; i < argc; i++) {
            char* a = argv[i++];
            assert(i < argc);
            char* n = argv[i];

            if (strcmp(a, "-file") == 0) {
                file = n;
            } else if (strcmp(a, "-pseudo") == 0) {
                pseudo = (strcmp(n, "true")==0);
            } else if (strcmp(a, "-node")==0) {
                num_nodes = atol(n);
            } else if (strcmp(a, "-index")==0) {
                index = atol(n);
            } else if (strcmp(a,"-port")==0) {
                port = atol(n);
            } else if (strcmp(a, "-masterip")==0) {
                master_ip = n;
            } else if (strcmp(a, "-app")==0) {
                app = n;
            } else if (strcmp(a,"=masterport")==0) {
                master_port = atol(n);
            } else if (strcmp(a, "rowsperchunk")==0) {
                rows_per_chunk = atol(n);
            } else {
                cout << "Unknown command line: " << a << " " << n << endl;
            }
        }
    }

};

extern Args arg;