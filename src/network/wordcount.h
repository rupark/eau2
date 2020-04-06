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
    void become(const char* v) {
        size_ = strlen(v);
        cstr_ = (char*) v;
        hash_ = hash_me();
    }
};

class KeyBuff : public Object {
public:
    Key* orig_; // external
    StrBuff buf_;

    KeyBuff(Key* orig) {
        orig_ = new Key(orig);
        buf_ = *new StrBuff(orig->c_str());
    }

    KeyBuff& c(String &s) { buf_.c(s); return *this;  }
    KeyBuff& c(size_t v) { buf_.c(v); return *this; }
    KeyBuff& c(const char* v) { buf_.c(v); return *this; }

    Key* get() {
        cout << "key buff get";
        //buf_.c(orig_->c_str());
        String* s = buf_.get();
        Key* k = new Key(s->steal(), orig_->home);
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
class WordCount: public Application {
public:
    static const size_t BUFSIZE = 1024;
    Key in;
    KeyBuff kbuf;
    SIMap all;

    WordCount(size_t idx, NetworkIP & net):
            Application(idx, net), in("data"), kbuf(new Key("wc-map-",0)) { }

    /** The master nodes reads the input, then all of the nodes count. */
    void run_() override {
        if (idx_ == 0) {
            cout << "In server run" << endl;
            FileReader fr = *new FileReader();
//            delete DataFrame::fromVisitor(&in, &kv, "S", fr);
            DataFrame::fromVisitor(&in, &kv, "S", fr);
        }
        local_count();
        reduce();
        cout << "finished reduce" << endl;
    }

    /** Returns a key for given node.  These keys are homed on master node
     *  which then joins them one by one. */
    Key* mk_key(size_t idx) {
        cout << "called mk_key idx = " << idx << endl;
        Key * k = kbuf.c(idx).get();
        //LOG("Created key " << k->c_str());
        cout << "Created key " << k->c_str() << endl;
        return k;
    }

    /** Compute word counts on the local node and build a data frame. */
    void local_count() {
        cout <<"in local count" << endl;
//        DataFrame* words = kv.waitAndGet(in);
        DataFrame* words = kv.get(in);
        cout << "got words" << endl;
        //p("Node ").p(index).pln(": starting local count...");
        cout << "Node " << this_node() << ": starting local count..." << endl;
        SIMap map;
        Adder add(map);
        cout << "adder created" << endl;
        words->local_map(add);
        cout << "local mapped adder" << endl;
        //delete words;
        //cout << "delete words" << endl;
        Summer cnt(map);
        cout << "created summer" << endl;
        DataFrame::fromVisitor(mk_key(this_node()), &kv, "SI", cnt);
        cout << "from visitor summer done" << endl;
    }

    /** Merge the data frames of all nodes */
    void reduce() {
        if (this_node() != 0) return;
//        pln("Node 0: reducing counts...");
        cout << "Node 0: reducing counts..." << endl;
        SIMap map;
        kbuf = new Key("wc-map-",0);
        cout << map.capacity_ << endl;
        Key* own = mk_key(0);
        cout << own->name << endl;
        merge(kv.get(*own), map);
        cout << "merge finish...map size=" << map.size() << endl;
        cout << "starting loop w/ num nodes=" << arg.num_nodes << endl;
        for (size_t i = 1; i < arg.num_nodes; ++i) { // merge other nodes
            cout << "in loop" << endl;
            Key* ok = mk_key(i);
            merge(kv.waitAndGet(*ok), map);
            delete ok;
        }
        cout << "Different words: " << map.size() << "!!" << endl;

        kv.get(*own)->print();

        cout << "done printing" << endl;
        //delete own;
        //cout << "deleted own" << endl;
    }

    void merge(DataFrame* df, SIMap& m) {
        cout << df->ncol << endl;
        cout <<"MERGING\n\n\n\n" << endl;
        cout << df->nrow << endl;
        cout << "schema: " << endl;
        cout << "string (0,0): " << df->get_string(0,0)->c_str() << endl;
        cout << "string (0,1): " << df->get_string(0,1)->c_str() << endl;
        cout << "string (1,0): " << df->get_int(1,0)<< endl;
        cout << "string (1,1): " << df->get_int(1,1) << endl;
        cout << "SI MAP size: " << m.size() << endl;

        cout << "welcome map: " << (m.get(*new String("the")) == nullptr) << endl;
        Adder add(m);
        cout << "adder created" << endl;
        df->map(add);

        cout << "MAP SIZE: " << m.size() << endl;
        //delete df;
    }

}; // WordcountDemo