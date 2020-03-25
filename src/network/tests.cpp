#include <stdio.h>
#include <iostream>
#include "serial.h"
#include <string.h>
using namespace std;

int main(int argc, char const *argv[])
{
    //TESTING SERIALIZATION AND DESERIALIZATION

    Status* s = new Status(0, 1, new String("hello"));
    Status* f = new Status(s->serialize()->cstr_);
    if (strcmp(s->serialize()->cstr_,f->serialize()->cstr_) == 0) {
        cout << "Status OK" << endl;
    }

    Register* r = new Register(0, 1, 8080, new String("127.0.0.1"));
    Register* e = new Register(r->serialize()->cstr_);
    if (strcmp(r->serialize()->cstr_,e->serialize()->cstr_) == 0) {
        cout << "Register OK" << endl;
    }

    Ack* a = new Ack(0, 1);
    Ack* g = new Ack(a->serialize()->cstr_);
    if (strcmp(a->serialize()->cstr_,g->serialize()->cstr_) == 0) {
        cout << "Ack OK" << endl;
    }

    size_t* ports = new size_t[3];
    ports[0] = 1;
    ports[1] = 1;
    ports[2] = 1;
    String** add = new String*[3];
    add[0] = new String("127.0.0.1");
    add[1] = new String("127.0.0.2");
    add[2] = new String("127.0.0.3");
    Directory* d = new Directory(0, 1, 3, ports, add);
    Directory* c = new Directory(d->serialize()->cstr_);
    if (strcmp(d->serialize()->cstr_,c->serialize()->cstr_) == 0) {
        cout << "Directory OK" << endl;
    }
}