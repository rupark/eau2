//
// Created by kramt on 3/25/2020.
//

#pragma once
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "serial.h"
#include "../string.h"
#include <iostream>
using namespace std;

/**
 * NodeInfo: each node is identified
 * by its node id (also called index) and its socket address.
 */
class NodeInfo : public Object {
public:
    unsigned id;
    sockaddr_in address;
};

/**
 * IP based network communications layer. Each node has an index
 * between 0 and num_nodes-1. nodes directory is ordered by node
 * index. Each node has a socket and ip address.
*/
class NetworkIP {
public:
    NodeInfo* nodes_;
    size_t this_node_;
    int sock_;
    sockaddr_in ip_;

    ~NetworkIP() {
        close(sock_);
    }

    NetworkIP() {

    }

    /**
     *
     * Returns this node's index.
     */
    size_t index() {
        return this_node_;
    }

    /**
     * Initialize node 0.
     */
   void server_init(unsigned idx, unsigned port) {
       this_node_ = idx;
       assert(idx==0 && "Server must be 0");
       init_sock_(port);
       nodes_ = new NodeInfo[3];

       for (size_t i = 0; i < 3; ++i) nodes_[i].id = 0;

       nodes_[0].address = ip_;
       nodes_[0].id = 0;

       //cout << nodes_[0].address.sin_addr.s_addr << endl;
       for (size_t i =1; i < 3; i++) {
           Register* msg = dynamic_cast<Register*>(recv_m());
           nodes_[msg->sender_].id = msg->sender_;
           nodes_[msg->sender_].address.sin_family = AF_INET;
           //nodes_[msg->sender_].address.sin_addr = msg->client.sin_addr;
           cout << "sender:" << msg->sender_ << endl;
           if (i == 1 && inet_pton(AF_INET, "127.0.0.2", &nodes_[msg->sender_].address.sin_addr) <= 0) {
               assert(false && "Invalid server IP address format");
           }
           if (i == 2 && inet_pton(AF_INET, "127.0.0.3", &nodes_[msg->sender_].address.sin_addr) <= 0) {
               assert(false && "Invalid server IP address format");
           }
           nodes_[msg->sender_].address.sin_port = htons(msg->port);
       }
       cout << "finished for loop nodes" << endl;
       size_t* ports = new size_t[2];
       String** addresses = new String*[2];
        cout << "created addresses ports arrays" << endl;
       for (size_t i = 0; i < 2; i++) {
           ports[i] = ntohs(nodes_[i + 1].address.sin_port);
           addresses[i] = new String(inet_ntoa(nodes_[i + 1].address.sin_addr));
       }
       cout << "finished addresses ports arrays loop" << endl;

       for (int i = 0 ; i < 3; i++) {
           cout << inet_ntoa(nodes_[0].address.sin_addr) << endl;
       };
       Directory ipd(ports, addresses);
       cout << ipd.nodes << endl;
       //ipd.log();
       for (size_t i = 0; i < 2; i++) {
           ipd.target_ = i;
           cout << "Server sending directory" << endl;
           sleep(3);
           send_m(&ipd);
       }

   }

   /** Intializes a client node */
   void client_init(unsigned idx, unsigned port, char* server_adr,
           unsigned server_port, char* client_adr) {
       this_node_ = idx;
//       init_sock_(port);

       inet_aton(client_adr, (struct in_addr *)&ip_.sin_addr.s_addr);
       assert((sock_ = socket(AF_INET, SOCK_STREAM, 0)) >= 0);
       int opt = 1;
       assert(setsockopt(sock_,
                         SOL_SOCKET, SO_REUSEADDR,
                         &opt, sizeof(opt)) == 0);
       ip_.sin_family = AF_INET;
       //ip_.sin_addr.s_addr = INADDR_ANY;
       cout << "From Init Sock: " << endl;
       ip_.sin_port = htons(port);
       assert(bind(sock_, (sockaddr*) &ip_, sizeof(ip_)) >= 0);
       cout << inet_ntoa(ip_.sin_addr) << endl;
       assert(listen(sock_, 100) >= 0);

       nodes_ = new NodeInfo[1];
       nodes_[0].id = 0;
       nodes_[0].address.sin_family = AF_INET;
       nodes_[0].address.sin_port = htons(server_port);
       if (inet_pton(AF_INET, server_adr, &nodes_[0].address.sin_addr) <= 0) {
           assert(false && "Invalid server IP address format");
       }
       //TODO how to convert idx to sock_addr_in?????
       Register msg(idx, port, ip_);
       send_m(&msg);
       Directory* ipd = dynamic_cast<Directory*>(recv_m());
       cout << ipd->nodes << endl;
        for (int i = 0; i < ipd->nodes; i++) {
            cout << "port: " << ipd->ports[i] << " add: " << ipd->addresses[i]->cstr_ << endl;
        }
       NodeInfo* nodes = new NodeInfo[ipd->nodes + 1];
       nodes[0] = nodes_[0];
       for (size_t i = 0; i < ipd->nodes; i++) {
           nodes[i+1].id = i+1;
           nodes[i+1].address.sin_family = AF_INET;
           nodes[i+1].address.sin_port = htons(ipd->ports[i]);
           if (inet_pton(AF_INET, ipd->addresses[i]->c_str(),
                   &nodes[i+1].address.sin_addr) <= 0) {
               cout << "Invalid IP direcotry-addr" << endl;
           }
       }

       cout << "server add: " << inet_ntoa(nodes[0].address.sin_addr) << endl;
       cout << "server id: " << nodes[0].id << endl;
       cout << "client add: " << inet_ntoa(nodes[1].address.sin_addr) << endl;
       cout << "client id: " << nodes[1].id << endl;

       delete[] nodes_;
       nodes_ = nodes;
       delete ipd;
   }

   /** Create a socket and bind it. */
   void init_sock_(unsigned port) {
       assert((sock_ = socket(AF_INET, SOCK_STREAM, 0)) >= 0);
       int opt = 1;
       assert(setsockopt(sock_,
               SOL_SOCKET, SO_REUSEADDR,
               &opt, sizeof(opt)) == 0);
       ip_.sin_family = AF_INET;
       ip_.sin_addr.s_addr = INADDR_ANY;
       cout << "From Init Sock: " << endl;
       ip_.sin_port = htons(port);
       assert(bind(sock_, (sockaddr*) &ip_, sizeof(ip_)) >= 0);
       cout << inet_ntoa(ip_.sin_addr) << endl;
       assert(listen(sock_, 100) >= 0);
   }

   /** Based on the message target, creates new connection to the appropriate
    * server and then serializes the message on the connection fd. **/
   void send_m(Message* msg) {
       cout << "in send_m" << endl;
       NodeInfo & tgt = nodes_[msg->target_];
       cout << "found tgt = " << inet_ntoa(tgt.address.sin_addr) << endl;
       int conn = socket(AF_INET, SOCK_STREAM, 0);
       assert(conn >= 0 && "Unable to create client socket");
       cout << "socket created" << endl;
       if (connect(conn, (sockaddr*)&tgt.address, sizeof(tgt.address)) < 0) {
           cout << "Unable to connect to remote node" << endl;
       }
       cout << "connected" << inet_ntoa(tgt.address.sin_addr) << endl;
       String* msg_ser =  msg->serialize();
       char* buf = msg_ser->c_str();
       size_t size = msg_ser->size();
       send(conn, &size, sizeof(size_t), 0);
       send(conn, buf, size, 0);
       cout << "finished sending msg" << endl;
   }

   /** Listens on the server socket. When a message becomes available, reads
    * its data, deserialize it and return object. */
   Message* recv_m() {
       cout << "in recv_m()" << endl;
       sockaddr_in sender;
       socklen_t addrlen = sizeof(sender);
       cout << "sock = " << sock_ << endl;
       int req = accept(sock_, (sockaddr*) &sender, &addrlen);
       cout << "accepted connection" << endl;
       size_t size = 0;
       if (read(req, &size, sizeof(size_t))  == 0) {
           cout << "failed to read" << endl;
       }
       cout << "reading size " << size << endl;
       char* buf = new char[size];
       int rd = 0;
       while(rd != size) {
           rd+= read(req, buf + rd, size - rd);
       }
       cout << "finished reading" << endl;
       Message* msg;

       cout << "buf: " << buf << endl;
       // SWITCH STATEMENT
       switch(buf[0]) {
           case '1': // Register
                cout << "receiving register" << endl;
               msg = new Register(buf);
               break;
           case '2': // ACK
               msg = new Ack(buf);
               break;
           case '3': // Status
               msg = new Status(buf);
               break;
           case '4': // Directory
               msg = new Directory(buf);
               break;
       }

       return msg;
   }

};