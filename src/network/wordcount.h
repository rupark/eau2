#pragma once

#include "application.h"
#include "../reader.h"
#include "../dataframe.h"
#include "../row.h"
#include <stdio.h>
#include <stdlib.h>
#include "../string.h"
#include "../key.h"
#include "../kvstore.h"
#include "../helper.h"
#include "../array.h"
#include "../args.h"
#include "../writer.h"
#include "../SImap.h"
#include <iostream>

using namespace std;

Args arg;

/***************************************************************************/

class MutableString : public String {
public:
    MutableString() : String("", 0) {}

    void become(const char *v) {
        size_ = strlen(v);
        cstr_ = (char *) v;
        hash_ = hash_me();
    }
};

class KeyBuff : public Object {
public:
    Key *orig_; // external
    StrBuff buf_;

    KeyBuff(Key *orig) {
        orig_ = new Key(orig);
        buf_ = *new StrBuff(orig->c_str());
    }

    KeyBuff &c(String &s) {
        buf_.c(s);
        return *this;
    }

    KeyBuff &c(size_t v) {
        buf_.c(v);
        return *this;
    }

    KeyBuff &c(const char *v) {
        buf_.c(v);
        return *this;
    }

    Key *get() {
        //buf_.c(orig_->c_str());
        String *s = buf_.get();
        Key *k = new Key(s->steal(), orig_->home);
        delete s;
        return k;
    }
}; // KeyBuff

/****************************************************************************
 * Calculate a word count for given file:
 *   1) read the data (single node)
 *   2) produce word counts per homed chunks, in parallel
 *   3) combine the results
 **********************************************************author: pmaj ****/
class WordCount : public Application {
public:
    static const size_t BUFSIZE = 1024;
    Key in;
    KeyBuff kbuf;
    SIMap all;

    WordCount(size_t idx, NetworkIP &net) :
            Application(idx, net), in("data"), kbuf(new Key("wc-map-", 0)) {}

//TODO IMPLEMENT THIS
//    /** The master nodes reads the input, then all of the nodes count. */
//    void run_() override {
//        if (idx_ == 0) {
//            cout << "In server run" << endl;
//            FileReader fr = *new FileReader();
//
//            DataFrame *df = DataFrame::fromVisitor(&in, &kv, "S", fr);
//
//            int num_chunks = ceil(df->nrow / arg.rows_per_chunk);
//            int selectedNode = 1;
//            for (int j = 0; j < num_chunks; j++) {
//                DataFrame *chunk = df.chunk(j);
//                Status chunkMsg(selectedNode, 0, chunk);
//                cout << "Server sending chunk" << endl;
//                sleep(3);
//                send_m(&chunkMsg);
//                selectedNode++;
//                if (selectedNode == arg.num_nodes) {
//                    selectedNode = 1;
//                }
//            }
//
//            //HERE IS WHERE WE RECIEVE EVERYONE AND ADD THEIR DFs to the KV TODO
//            for (size_t i = 1; i < arg.num_nodes; i++) {
//                Status *msg = dynamic_cast<Status *>(n.recv_m());
//                this->kv.put(mk_key(this->idx_), msg->msg_);
//            }
//
//            //Theoretically everyone should now be in the store to reduce
//            reduce();
//            cout << "finished reduce" << endl;
//
//        } else {
//            Status *ipd = dynamic_cast<Status *>(recv_m()); // Put this in Kv?
//            local_count();
//            Status msg(this->idx_, 0, kv.get(mk_key(this->idx_)));
//            send_m(&msg);
//            cout << "DONE" << endl;
//        }
//    }

    /** The master nodes reads the input, then all of the nodes count. */
    void run_() override {
        if (idx_ == 0) {
            FileReader fr = *new FileReader();
//            delete DataFrame::fromVisitor(&in, &kv, "S", fr);
            DataFrame::fromVisitor(&in, &kv, "S", fr);
        }
        local_count();
        reduce();

    }

    /** Returns a key for given node.  These keys are homed on master node
     *  which then joins them one by one. */
    Key *mk_key(size_t idx) {
        Key *k = kbuf.c(idx).get();
        cout << "Created key " << k->c_str() << endl;
        return k;
    }

    /** Compute word counts on the local node and build a data frame. */
    void local_count() {
//      DataFrame* words = kv.waitAndGet(in);
        DataFrame *words = kv.get(in);
        cout << "Node " << this_node() << ": starting local count..." << endl;
        SIMap map;
        Adder add(map);
        words->local_map(add);
        //delete words;
        Summer cnt(map);
        DataFrame::fromVisitor(mk_key(this_node()), &kv, "SI", cnt);
    }

    /** Merge the data frames of all nodes */
    void reduce() {
        if (this_node() != 0) return;
        cout << "Node 0: reducing counts..." << endl;
        SIMap map;
        kbuf = new Key("wc-map-", 0);
        Key *own = mk_key(0);
        DataFrame* df = kv.get(*own);
        cout << "printing" << endl;
        cout << "ncol: " << df->ncol << endl;
        cout << "nrow: " << df->nrow << endl;
        merge(kv.get(*own), map);
        for (size_t i = 1; i < arg.num_nodes; ++i) { // merge other nodes
            Key *ok = mk_key(i);
            merge(kv.waitAndGet(*ok), map);
            delete ok;
        }
        cout << "Different words: " << map.size() << endl;

        //delete own;
    }

    /** Adds map values into dataframe */
    void merge(DataFrame *df, SIMap &m) {
        Adder add(m);
        df->map(add);
        //delete df;
    }

}; // WordcountDemo