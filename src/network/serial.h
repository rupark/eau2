#include "string.h"
#include "object.h"
#include <stdio.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
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
        //size_t id_;     // an id t unique within the node
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
    String* msg_; // owned

    Status(int sender, int target, String* msg) {
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
        this->kind_ = MsgKind::Status;
        this->sender_ = atoi(args[1]);
        this->target_ = atoi(args[2]);
        this->msg_ = new String(args[3]);
    }

    String* serialize() {
        StrBuff* s = new StrBuff();
        char str[10000] = ""; /* In fact not necessary as snprintf() adds the
                         0-terminator. */
        snprintf(str, sizeof str, "3?", this->sender_);
        s->c(str);
        snprintf(str, sizeof this->sender_, "%zu", this->sender_);
        s->c(str);
        snprintf(str, sizeof str, "?", this->sender_);
        s->c(str);
        snprintf(str, sizeof this->target_, "%zu", this->target_);
        s->c(str);
        snprintf(str, sizeof str, "?", this->sender_);
        s->c(str);
        snprintf(str, sizeof msg_, "%s", msg_->cstr_);
        s->c(str);
        return s->get();
    }

};

class Register : public Message {
public:

    size_t port;
    String* address;

    Register(int sender, int target, size_t port, String* address) {
        this->kind_ = MsgKind::Register;
        this->sender_ = sender;
        this->target_ = target;
        this->port = port;
        this->address = address;
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
        this->kind_ = MsgKind::Register;
        this->sender_ = atoi(args[1]);
        this->target_ = atoi(args[2]);
        this->port = atoi(args[3]);
        this->address = new String(args[4]);
    }

    String* serialize() {
        StrBuff* s = new StrBuff();
        char str[256] = ""; /* In fact not necessary as snprintf() adds the
                         0-terminator. */
        snprintf(str, sizeof str, "1?");
        s->c(str);
        snprintf(str, sizeof str, "%zu?", this->sender_);
        s->c(str);
        snprintf(str, sizeof str, "%zu?", this->target_);
        s->c(str);
        snprintf(str, sizeof str, "%zu?", port);
        s->c(str);
        snprintf(str, sizeof str, "%s", this->address->cstr_);
        s->c(str);
        return s->get();
    }

};

class Directory : public Message {
public:
    size_t nodes;
    size_t * ports;  // owned
    String ** addresses;  // owned; strings owned

    Directory(int sender, int target, size_t nodes, size_t * ports, String ** addresses) {
        this->kind_ = MsgKind::Directory;
        this->sender_ = sender;
        this->target_ = target;
        this->nodes = nodes;
        this->ports = ports;
        this->addresses = addresses;
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