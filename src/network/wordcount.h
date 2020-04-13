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
#include "../parser.h"
#include "../writer.h"
#include "../SImap.h"
#include <iostream>
#include <math.h>

using namespace std;

//Args arg;

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
            //cout << "In server run" << endl;
            // Reads in File to Dataframe
            FileReader fr = *new FileReader();
            DataFrame *df = DataFrame::fromVisitor(&words_all, &kv, "S", fr);

            // Split into chunks and send iteratively to nodes
            int num_chunks = ceil(df->nrow / arg.rows_per_chunk);
            //cout << "Num Chunks = " << num_chunks << endl;
            int selectedNode = 0;

            for (size_t j = 0; j < num_chunks; j++) {
                //cout << "building chunk " << j << endl;
                DataFrame *cur_chunk = df->chunk(j);
                //cout << "chunk built size = " << cur_chunk->nrows() << endl;
                //cout << "rowsperchunk = " << arg.rows_per_chunk << endl;
//                cur_chunk->print();
//                //cout << endl;

                // if server's turn, keep chunks of DataFrame.
                if (selectedNode == 0) {
                    // put chunks into local kv store as received
                    cout << "putting cur_chunk " << j << " in key" << in.name->c_str() << endl;
                    kv.put(&in, kv.get(in)->append_chunk(cur_chunk));
                    //cout << "chunk put" << endl;
                    //cout << "selected node = " << selectedNode << endl;
                } else {
                    // Sending to Clients
                    //cout << "(to) selected node = " << selectedNode << endl;
                    Status *chunkMsg = new Status(0, selectedNode, cur_chunk);
                    cout << "Server sending chunk" << endl;
                    ///sleep(3);
                    this->net.send_m(chunkMsg);
                }
                // incriment selected node circularly between nodes
                selectedNode = ++selectedNode == arg.num_nodes ? selectedNode = 0 : selectedNode++;
            }

            local_count();


            //HERE IS WHERE WE RECEIVE EVERYONE AND ADD THEIR DFs to the KV
            for (size_t i = 1; i < arg.num_nodes; i++) {
                Status *msg = dynamic_cast<Status *>(this->net.recv_m());
//                this->kv.put(mk_key(i), msg->msg_);

                //cout << "\n\n\nPRINTING CLIENT DF" << endl;
                //               msg->msg_->print();
                //cout << "\n\n\n\n";

                this->kv.put(new Key("wc-map-1"), msg->msg_); // TODO ABSTRACT!
            }

            //Theoretically everyone should now be in the store to reduce
            reduce();
            //cout << "finished reduce" << endl;

        } else {

            //This is a terrible way to go about this but it will work
            //Finding out size of file
            FILE *file = fopen(arg.file, "r");
            size_t file_size = ftell(file);
            SorParser parser(file, (size_t) 0, (size_t) file_size, (size_t) file_size);
            parser.guessSchema();
            parser.parseFile();
            DataFrame *d = new DataFrame(parser.getColumnSet(), parser._num_columns);

            //Calculating the number of chunks and figuring out how many go to this node
            int num_chunks = ceil(d->nrow / arg.rows_per_chunk);
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

            for (size_t i = 0; i < num_recieved; i++) {
                // TODO need to loop until received all?
                cout << "client waiting to receive chunk" << endl;
                Status *ipd = dynamic_cast<Status *>(this->net.recv_m()); // Put this in Kv?
                //cout << "nrows ipd: " << ipd->msg_->nrow << endl;
                cout << "client received" << endl;

                DataFrame *chunkSoFar = kv.get(*new Key("data"));
                chunkSoFar->append_chunk(ipd->msg_);
                //cout << "nrows chunksofar: " << chunkSoFar->nrow << endl;
                kv.put(new Key("data"), chunkSoFar); // check if put is needed? df pointer manipulated...
            }

            //cout << "rdy to local count" << endl;
            local_count();
            //cout << "local counted" << endl;
            Key *key_counts2 = mk_key(this->idx_);
            //cout << "made key counts 2 = " << key_counts2->name->c_str() << endl;
            DataFrame *storeDF = kv.get(key_counts2);
            if (storeDF == nullptr) {
                //cout << "STORE DF IS NULL!! =========================" << endl;
            }
            cout << "got df of size" << storeDF->nrows() << endl;
            Status msg(this->idx_, 0, storeDF);
            //cout << "msg constructed" << endl;
            this->net.send_m(&msg);
            cout << "sending chunk back" << endl;
            cout << "DONE" << endl;
        }
    }

//    /** The master nodes reads the input, then all of the nodes count. */
//    void run_() override {
//        //cout <<"in wc" <<endl;
//        if (idx_ == 0) {
//            FileReader fr = *new FileReader();
////            delete DataFrame::fromVisitor(&in, &kv, "S", fr);
//            DataFrame::fromVisitor(&in, &kv, "S", fr);
//        }
//        local_count();
//        reduce();
//
//    }

    /** Returns a key for given node.  These keys are homed on master node
     *  which then joins them one by one. */
    Key *mk_key(size_t idx) {
        kbuf = *new KeyBuff(new Key("wc-map-"));
        //cout << "adding to kbuf: " << idx << endl;
        Key *k = kbuf.c(idx).get();
        //cout << "Created key " << k->c_str() << endl;
        return k;
    }

    /** Compute word counts on the local node and build a data frame. */
    void local_count() {
//      DataFrame* words = kv.waitAndGet(in);
        DataFrame *words = kv.get(in);
        //cout << "made words" << endl;
        //cout << words->nrow << endl;
        //cout << words->ncol << endl;
        //cout << "Node " << this_node() << ": starting local count..." << endl;
        SIMap map;
        //cout << "map created" << endl;
        Adder add(map);
        //cout << "adder created" << endl;
        words->local_map(add);
        //cout << "local mapped" << endl;
        //delete words;
        Summer cnt(map);
        //cout << "summer created" << endl;
        Key *key_counts = mk_key(this_node());
        //cout << "created key " << key_counts->name->c_str() << endl;
        DataFrame::fromVisitor(key_counts, &kv, "SI", cnt);
        //cout << "df visited" << endl;
    }

    /** Merge the data frames of all nodes */
    void reduce() {
        if (this_node() != 0) return;
        cout << "Node 0: reducing counts..." << endl;
        SIMap map;
        kbuf = new Key("wc-map-", 0);
        Key *own = mk_key(0);
        DataFrame *df = kv.get(*own);
        merge(df, map);
        //cout << "done merging server MAP SIZE = " << map.size() << endl;

//        //cout << "ncol: " << df->ncol << endl;
//        //cout << "nrow: " << df->nrow << endl;
//        df->print();

        for (size_t i = 1; i < arg.num_nodes; ++i) { // merge other nodes
            //Key *ok = mk_key(i);
            Key *ok = new Key("wc-map-1"); // TODO ABSTRACT!
            merge(kv.get(*ok), map);
            //           delete ok;
        }
        cout << "Different words: " << map.size() << endl;

        cout << "my : " << map.get(*new String("my"))->v << endl;
        cout << "name : " << map.get(*new String("name"))->v << endl;
        cout << "is : " << map.get(*new String("is"))->v << endl;
        cout << "kate : " << map.get(*new String("kate"))->v << endl;
        cout << "and : " << map.get(*new String("and"))->v << endl;
        cout << "his : " << map.get(*new String("his"))->v << endl;
        cout << "mark : " << map.get(*new String("mark"))->v << endl;


        //delete own;
    }

    /** Adds map values into dataframe */
    void merge(DataFrame *df, SIMap &m) {
        //cout << "in merge-------SCHEMA: " << df->get_schema().types->c_str() << endl;
        Adder add(m);
        //cout << "starting map using adder" << endl;
        df->map(add);
        //cout << "done with map using adder" << endl;
        //delete df;
    }

}; // WordcountDemo