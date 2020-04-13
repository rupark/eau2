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
        buf_ = *new StrBuff("wc-map-");
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
    Key in; // mapped to words local count needed for (a subset of words_all).
    KeyBuff kbuf;
    SIMap all;
    Key words_all; // used by server to separate word chunks.

    WordCount(size_t idx, NetworkIP &net) :
            Application(idx, net), in("data"), kbuf(new Key("wc-map-", 0)), words_all("words-all") {
        kv.put(&in, new DataFrame(*new Schema("S")));
    }

    /** The master nodes reads the input, then all of the nodes count. */
    void run_() override {

        if (idx_ == 0) {
            // Reads in File to Dataframe
            FileReader fr = *new FileReader();
            DataFrame *df = DataFrame::fromVisitor(&words_all, &kv, "S", fr);

            // Split into chunks and send iteratively to nodes
            int num_chunks = 1 + ((df->nrow - 1) / arg.rows_per_chunk);
            int selectedNode = 0;

            for (size_t j = 0; j < num_chunks; j++) {
                DataFrame *cur_chunk = df->chunk(j);

                // if server's turn, keep chunks of DataFrame.
                if (selectedNode == 0) {
                    // put chunks into local kv store as received
                    DataFrame *chunkSoFar = kv.get(*new Key("data"));
                    chunkSoFar->append_chunk(cur_chunk);
                    kv.put(new Key("data"), chunkSoFar);
                } else {
                    // Sending to Clients
                    Status *chunkMsg = new Status(0, selectedNode, cur_chunk);
                    cout << "Server sending chunk" << endl;
                    this->net.send_m(chunkMsg);
                }
                // increment selected node circularly between nodes
                selectedNode = ++selectedNode == arg.num_nodes ? selectedNode = 0 : selectedNode++;
            }

            local_count();


            //HERE IS WHERE WE RECEIVE EVERYONE AND ADD THEIR DFs to the KV
            for (size_t i = 1; i < arg.num_nodes; i++) {
                Status *msg = dynamic_cast<Status *>(this->net.recv_m());
                cout << msg->sender_ << endl;
                StrBuff *s = new StrBuff();
                s->c("wc-map-");
                s->c(msg->sender_);
                this->kv.put(new Key(s->get()), msg->msg_);
            }

            //Theoretically everyone should now be in the store to reduce
            reduce();

        } else {

            //Finding out size of file
            FileReader fr = *new FileReader();
            DataFrame *df = DataFrame::fromVisitor(&words_all, &kv, "S", fr);

            //Calculating the number of chunks and figuring out how many go to this node
            int num_chunks = 1 + ((df->nrow - 1) / arg.rows_per_chunk);
            int num_received = 0;
            int selectedNode = 0;
            for (int i = 0; i < num_chunks; i++) {
                if (selectedNode == idx_) {
                    num_received++;
                }
                selectedNode++;
                if (selectedNode > arg.num_nodes - 1) {
                    selectedNode = 0;
                }
            }

            for (size_t i = 0; i < num_received; i++) {
                cout << "client waiting to receive chunk" << endl;
                Status *ipd = dynamic_cast<Status *>(this->net.recv_m());
                cout << "client received" << endl;

                DataFrame *chunkSoFar = kv.get(*new Key("data"));
                chunkSoFar->append_chunk(ipd->msg_);
                kv.put(new Key("data"), chunkSoFar);
            }

            local_count();
            StrBuff *s = new StrBuff();
            s->c("wc-map-");
            s->c(this->idx_);
            Key *key_counts2 = new Key(s->get());
            DataFrame *storeDF = kv.get(key_counts2);
            Status msg(this->idx_, 0, storeDF);
            this->net.send_m(&msg);
            cout << "sending chunk back" << endl;
            cout << "DONE" << endl;
        }
    }

    /** Returns a key for given node.  These keys are homed on master node
     *  which then joins them one by one. */
    Key *mk_key(size_t idx) {
        kbuf = *new KeyBuff(new Key("wc-map-"));
        Key *k = kbuf.c(idx).get();
        return k;
    }

    /** Compute word counts on the local node and build a data frame. */
    void local_count() {
        DataFrame *words = kv.get(in);
        SIMap map;
        Adder add(map);
        words->local_map(add);
        Summer cnt(map);

        StrBuff *s = new StrBuff();
        s->c("wc-map-");
        s->c(this->idx_);
        Key *key_counts = new Key(s->get());

        DataFrame::fromVisitor(key_counts, &kv, "SI", cnt);
    }

    /** Merge the data frames of all nodes */
    void reduce() {
        if (this_node() != 0) return;
        cout << "Node 0: reducing counts..." << endl;
        SIMap map;

        StrBuff *s = new StrBuff();
        s->c("wc-map-");
        s->c(this->idx_);
        Key *own = new Key(s->get());
        DataFrame *df = kv.get(*own);
        merge(df, map);
        cout << map.get(*new String("nibh"))->v << endl;

        for (size_t i = 1; i < arg.num_nodes; ++i) { // merge other nodes
            StrBuff *s = new StrBuff();
            s->c("wc-map-");
            s->c(i);

            Key *ok = new Key(s->get());
            merge(kv.get(*ok), map);
        }

        cout << "Different words: " << map.size() << endl;

    }

    /** Adds map values into dataframe */
    void merge(DataFrame *df, SIMap &m) {
        Adder add(m);
        df->map(add);
    }

}; // WordcountDemo