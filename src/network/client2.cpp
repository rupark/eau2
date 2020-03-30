#include <iostream>
#include <string>
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <netdb.h>
#include <sys/uio.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <fstream>
#include "network.h"
#include "../string.h"
using namespace std;
//Client side
int main(int argc, char *argv[])
{
    NetworkIP* client = new NetworkIP();
    client->client_init(2, 8080, "127.0.0.1", 8080, "127.0.0.3");

    String* exp_s0 = new String("127.0.0.1");
    String* exp_c1 = new String("127.0.0.2");
    String* exp_c2 = new String("127.0.0.3");

    String* s0 = new String(inet_ntoa(client->nodes_[0].address.sin_addr));
    String* c1 = new String(inet_ntoa(client->nodes_[1].address.sin_addr));
    String* c2 = new String(inet_ntoa(client->nodes_[2].address.sin_addr));

    assert(s0->equals(exp_s0));
    cout << "Client2: ASSERT PASSED nodes_[0]: " << s0->c_str() << " = " << exp_s0->c_str() << endl;
    assert(c1->equals(exp_c1));
    cout << "Client2: ASSERT PASSED nodes_[1]: " << c1->c_str() << " = " << exp_c1->c_str() << endl;
    assert(c2->equals(exp_c2));
    cout << "Client2: ASSERT PASSED nodes_[2]: " << c2->c_str() << " = " << exp_c2->c_str() << endl;

    cout << "Client2 DONE" << endl;

    delete exp_s0;
    delete exp_c1;
    delete exp_c2;
    delete s0;
    delete c1;
    delete c2;
//    delete client;

//    //we need 2 things: ip address and port number, in that order
//    if(argc != 3)
//    {
//        cerr << "Usage: ip_address port" << endl; exit(0);
//    } //grab the IP address and port number
//    char *serverIp = argv[1]; int port = atoi(argv[2]);
//    //create a message buffer
//    char msg[1500];
//    //setup a socket and connection tools
//    struct hostent* host = gethostbyname(serverIp);
//    sockaddr_in sendSockAddr;
//    bzero((char*)&sendSockAddr, sizeof(sendSockAddr));
//    sendSockAddr.sin_family = AF_INET;
//    sendSockAddr.sin_addr.s_addr =
//            inet_addr(inet_ntoa(*(struct in_addr*)*host->h_addr_list));
//    sendSockAddr.sin_port = htons(port);
//    int clientSd = socket(AF_INET, SOCK_STREAM, 0);
//    //try to connect...
//    int status = connect(clientSd,
//                         (sockaddr*) &sendSockAddr, sizeof(sendSockAddr));
//    if(status < 0)
//    {
//        cout<<"Error connecting to socket!"<<endl;
//    }
//    cout << "Connected to the server!" << endl;
//    int bytesRead, bytesWritten = 0;
//    struct timeval start1, end1;
//    gettimeofday(&start1, NULL);
//    while(1)
//    {
//        cout << ">";
//        string data;
//        getline(cin, data);
//        memset(&msg, 0, sizeof(msg));//clear the buffer
//        strcpy(msg, data.c_str());
//        if(data == "exit")
//        {
//            send(clientSd, (char*)&msg, strlen(msg), 0);
//            break;
//        }
//        bytesWritten += send(clientSd, (char*)&msg, strlen(msg), 0);
//        cout << "Awaiting server response..." << endl;
//        memset(&msg, 0, sizeof(msg));//clear the buffer
//        bytesRead += recv(clientSd, (char*)&msg, sizeof(msg), 0);
//        if(!strcmp(msg, "exit"))
//        {
//            cout << "Server has quit the session" << endl;
//            break;
//        }
//        cout << "Server: " << msg << endl;
//    }
//    gettimeofday(&end1, NULL);
//    close(clientSd);
//    cout << "********Session********" << endl;
//    cout << "Bytes written: " << bytesWritten <<
//         " Bytes read: " << bytesRead << endl;
//    cout << "Elapsed time: " << (end1.tv_sec- start1.tv_sec)
//         << " secs" << endl;
//    cout << "Connection closed" << endl;
//    return 0;
}