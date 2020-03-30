#include "../string.h"
#include "../object.h"
#include <stdio.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "../dataframe.h"
#pragma once

#include <iostream>
using namespace std;

enum class MsgKind { Ack, Nack, Put,
    Reply,  Get, WaitAndGet, Status,
    Kill,   Register,  Directory };

class Message : public Object {
    public:
        MsgKind kind_;  // the message kind
        size_t sender_; // the index of the sender node
        size_t target_; // the index of the receiver node
        size_t id_;     // an id t unique within the node
        virtual String* serialize() {
            return nullptr;
        }
};

class Ack : public Message {
public:

    Ack(int sender, int target) {
        this->kind_ = MsgKind::Ack;
        this->sender_ = sender;
        this->target_ = target;
    }

    Ack(char* buffer) {
        char** args = new char*[1000];
        char *token = strtok(buffer,"?");
        int i = 0;
        while (token != NULL)
        {
            args[i] = token;
            i++;
            token = strtok (NULL, "?");
        }
        this->kind_ = MsgKind::Ack;
        this->sender_ = atoi(args[1]);
        this->target_ = atoi(args[2]);
    }

    String* serialize() {
        StrBuff* s = new StrBuff();
        char str[256] = ""; /* In fact not necessary as snprintf() adds the
                         0-terminator. */
        s->c("2?");
        snprintf(str, sizeof this->sender_, "%zu?", this->sender_);
        s->c(str);
        snprintf(str, sizeof this->target_, "%zu?", this->target_);
        s->c(str);
        return s->get();
    }

};

class Status : public Message {
public:
    DataFrame* msg_; // owned

    Status(int sender, int target, DataFrame* msg) {
        this->kind_ = MsgKind::Status;
        this->sender_ = sender;
        this->target_ = target;
        this->msg_ = msg;
    }

    Status(char* buffer) {
        cout << "here" << endl;
        char** args = new char*[1000];
        char *token = strtok(buffer,"?");
        int i = 0;
        while (token != NULL)
        {
            args[i] = token;
            i++;
            token = strtok (NULL, "?");
        }
        cout << "broke up tokens" << endl;
        this->kind_ = MsgKind::Status;
        this->sender_ = atoi(args[1]);
        this->target_ = atoi(args[2]);

        char* recieved = args[3];
        char** columns = new char*[1000];
        size_t columns_size = 0;
        token = strtok(recieved,"!");
        while (token != NULL)
        {
            cout << token << endl;
            columns[i] = token;
            columns_size++;
            token = strtok (NULL, "!");
        }
        cout << "broke up into columns" << endl;

        DataFrame* d = new DataFrame(*new Schema());
        cout << columns_size << endl;
        for (int i = 0; i < columns_size; i++) {
            cout << "in for loop" << endl;
            Column* c;
            char* column = columns[i];
            cout << column << endl;
            token = strtok(column,"}");
            cout << token << endl;
            switch (token[0]) {
                case 'F':
                    cout << "float col" << endl;
                    token = strtok(column,"}");
                    c = new FloatColumn();
                    while (token != NULL)
                    {
                        c->push_back((float)atof(token));
                        columns_size++;
                        token = strtok (NULL, "}");
                    }
                    break;
                case 'S':
                    cout << "str col" << endl;
                    token = strtok(column,"}");
                    c = new StringColumn();
                    while (token != NULL)
                    {
                        c->push_back(new String(token));
                        columns_size++;
                        token = strtok (NULL, "}");
                    }
                    break;
                case 'B':
                    cout << "bool col" << endl;
                    token = strtok(column,"}");
                    c = new BoolColumn();
                    while (token != NULL)
                    {
                        c->push_back((bool)atoi(token));
                        columns_size++;
                        token = strtok (NULL, "}");
                    }
                    break;
                case 'I':
                    cout << "int col" << endl;
                    token = strtok(column,"}");
                    c = new IntColumn();
                    while (token != NULL)
                    {
                        c->push_back((int)atoi(token));
                        columns_size++;
                        token = strtok (NULL, "}");
                    }
                    break;
            }
            d->add_column(c, new String(""));
        }

        cout << "populated columns" << endl;

        this->msg_ = d;

    }

    String* serialize() {
        cout << "making strbuff" << endl;
        StrBuff* s = new StrBuff();
        cout << "making str" << endl;
        char str[10000] = ""; /* In fact not necessary as snprintf() adds the
                         0-terminator. */
        cout << "adding index" << endl;
        snprintf(str, sizeof str, "3?");
        s->c(str);
        cout << "adding sender" << endl;
        snprintf(str, sizeof this->sender_, "%zu", this->sender_);
        s->c(str);
        cout << "adding ?" << endl;
        snprintf(str, sizeof str, "?");
        s->c(str);
        cout << "adding target" << endl;
        snprintf(str, sizeof this->target_, "%zu", this->target_);
        s->c(str);
        cout << "adding ?" << endl;
        snprintf(str, sizeof str, "?");
        s->c(str);
        cout << msg_->ncol << endl;
        for (int i = 0; i < msg_->ncol; i++) {
            snprintf(str, sizeof str, "%s", msg_->columns[i]->serialize()->cstr_);
            s->c(str);
        }
        return s->get();
    }

};

class Register : public Message {
public:

    size_t idx;
    sockaddr_in client;
    size_t port;

    Register(unsigned idx, size_t port, sockaddr_in ip_) {
        this->kind_ = MsgKind::Register;
        this->sender_ = idx;
        this->target_ = 0;
        this->idx = idx;
        this->client = ip_;
        this->port = (size_t)port;
    }

    Register(char* buffer) {
        char** args = new char*[1000];
        char *token = strtok(buffer,"?");
        int i = 0;
        while (token != NULL)
        {
            args[i] = token;
            i++;
            token = strtok (NULL, "?");
        }
        cout << "tokenized register" << endl;
        this->kind_ = MsgKind::Register;
        this->sender_ = atoi(args[1]);
        cout << "args[1] done" << endl;
        this->target_ = atoi(args[2]);
        cout << "args[2] done" << endl;
        this->idx = atoi(args[3]);
        cout << "args[3] done" << endl;
        struct sockaddr_in myaddr;
        cout << "args[4] = " << atoi(args[4]) << endl;
        myaddr.sin_family = atoi(args[4]);
        cout << "args[4] done" << endl;
        myaddr.sin_port = atoi(args[5]);
        cout << "args[5] done" << endl;
        inet_aton(args[6], (struct in_addr *)&myaddr.sin_addr.s_addr);
//        myaddr.sin_addr.s_addr = (in_addr_t) atol(args[6]);
//        inet_aton(args[6], &myaddr.sin_addr.s_addr);
        cout << "args[6]" << args[6] << endl;
        cout << "args[6] done" << endl;
        this->client = myaddr;
        this->port = atoi(args[7]);
        cout << "args[7] done" << endl;
    }

    String* serialize() {
        StrBuff* s = new StrBuff();
        char str[1024] = ""; /* In fact not necessary as snprintf() adds the
                         0-terminator. */
        snprintf(str, sizeof str, "1?");
        s->c(str);
        snprintf(str, sizeof str, "%zu?", this->sender_);
        s->c(str);
        snprintf(str, sizeof str, "%zu?", this->target_);
        s->c(str);
        snprintf(str, sizeof str, "%zu?", this->idx);
        s->c(str);
        snprintf(str, sizeof str, "%d?", this->client.sin_family);
        s->c(str);
        snprintf(str, sizeof str, "%d?", this->client.sin_port);
        s->c(str);
        snprintf(str, sizeof str, "%s?", inet_ntoa(client.sin_addr));
        s->c(str);
        snprintf(str, sizeof str, "%zu", this->port);
        s->c(str);
        return s->get();
    }

};

class Directory : public Message {
public:
    size_t nodes;
    size_t * ports;  // owned
    String ** addresses;  // owned; strings owned

    Directory(size_t * ports, String ** addresses, size_t nodes) {
        this->kind_ = MsgKind::Directory;
        this->sender_ = 0;
        this->target_ = 0;
        this->nodes = nodes;
        this->ports = ports;
        this->addresses = addresses;
        cout << "finished creating dir" << endl;
    }

    Directory(char* buffer) {
        char** args = new char*[1000];
        char *token = strtok(buffer,"?");
        int i = 0;
        while (token != NULL)
        {
            args[i] = token;
            i++;
            token = strtok (NULL, "?");
        }
        this->kind_ = MsgKind::Directory;
        this->sender_ = atoi(args[1]);
        this->target_ = atoi(args[2]);
        this->nodes = atoi(args[3]);
        this->ports = new size_t[1000];
        this->addresses = new String*[1000];
        for (int i = 4; i < 4 + nodes; i++) {
            this->ports[i - 4] = atoi(args[i]);
        }
        for (int j = 4 + nodes; j < 4 + nodes + nodes; j++) {
            this->addresses[j - (4 + nodes)] = new String(args[j]);
        }
    }

    String* serialize() {
        StrBuff* s = new StrBuff();
        char str[256] = ""; /* In fact not necessary as snprintf() adds the
                         0-terminator. */

        snprintf(str, sizeof str, "4?");
        s->c(str);
        snprintf(str, sizeof str, "%zu?", this->sender_);
        s->c(str);
        snprintf(str, sizeof str, "%zu?", this->target_);
        s->c(str);
        snprintf(str, sizeof str, "%zu?", this->nodes);
        s->c(str);
        for (int i = 0; i < nodes; i++) {
            snprintf(str, sizeof ports[i], "%zu?", ports[i]);
            s->c(str);
        }
        for (int i = 0; i < nodes; i++) {
            snprintf(str, sizeof str, "%s", addresses[i]->cstr_);
            s->c(str);
            snprintf(str, sizeof str, "?");
            s->c(str);
        }
        return s->get();
    }
};