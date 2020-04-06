#pragma once
#include "application.h"
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
#include <iostream>
using namespace std;

Args arg;

/***********************************************x****************************
 *
 **********************************************************author:jvitek */
class Num : public Object {
public:
    size_t v = 0;
    Num() {}
    Num(size_t v) : v(v) {}
};

/**  Item_ are entries in a Map, they are not exposed, are immutable, own
 *   they key, but the value is external.  author: jv */
class Items_ {
public:
    Array keys_;
    Array vals_;

    Items_() : keys_(8), vals_(8) {}

    Items_(Object *k, Object * v) : keys_(8), vals_(8) {
        keys_.push_back(k);
        vals_.push_back(v);
    }

    bool contains_(Object& k) {
        for (int i = 0; i < keys_.size(); i++)
            if (k.equals(keys_.get_(i)))
                return true;
        return false;
    }

    Object* get_(Object& k) {
        for (int i = 0; i < keys_.size(); i++)
            if (k.equals(keys_.get_(i)))
                return vals_.get_(i);
        return nullptr;
    }

    size_t set_(Object& k, Object* v) {
        for (int i = 0; i < keys_.size(); i++)
            if (k.equals(keys_.get_(i))) {
                vals_.put(i,v);
                return 0;
            }
        // The keys are owned, but the key is received as a reference, i.e. not owned so we must make a copy of it.
        keys_.push_back(k.clone());
        vals_.push_back(v);
        return 1;
    }

    size_t erase_(Object& k) {
        for (int i = 0; i < keys_.size(); i++)
            if (k.equals(keys_.get_(i))) {
                keys_.erase_(i);
                vals_.erase_(i);
                return 1;
            }
        return 0;
    }
};

/** A generic map class from Object to Object. Subclasses are responsibly of
 * making the types more specific.  author: jv */
class Map : public Object {
public:
    size_t capacity_;
    // TODO this was not size of the map, but number of occupied item positions in the top level
    size_t size_ = 0;
    Items_* items_;  // owned

    Map() : Map(10) {}
    Map(size_t cap) {
        capacity_ = cap;
        items_ = new Items_[capacity_];
    }

    ~Map() { delete[] items_; }

    /** True if the key is in the map. */
    bool contains(Object& key)  { return items_[off_(key)].contains_(key); }

    /** Return the number of elements in the map. */
    size_t size()  {
        return size_;
    }

    size_t off_(Object& k) { return  k.hash() % capacity_; }

    /** Get the value.  nullptr is allowed as a value.  */
    Object* get_(Object &key) { return items_[off_(key)].get_(key); }

    /** Add item->val_ at item->key_ either by updating an existing Item_ or
     * creating a new one if not found.  */
    void set(Object &k, Object *v) {
        if (size_ >= capacity_)
            grow();
        size_ += items_[off_(k)].set_(k,v);
    }

    /** Removes element with given key from the map.  Does nothing if the
        key is not present.  */
    void erase(Object& k) {
        size_ -= items_[off_(k)].erase_(k);
    }

    /** Resize the map, keeping all Item_s. */
    void grow() {
        //LOG("Growing map from capacity " << capacity_);
        Map newm(capacity_ * 2);
        for (size_t i = 0; i < capacity_; i++) {
            size_t sz = items_[i].keys_.size();
            for (size_t j = 0; j < sz; j++) {
                Object* k = items_[i].keys_.get_(j);
                Object* v = items_[i].vals_.get_(j);
                newm.set(*k,v);
                // otherwise the values would get deleted (if the array's destructor was doing its job I found later:)
                items_[i].vals_.put(j, nullptr);
            }
        }
        delete[] items_;
        items_ = newm.items_;
        capacity_ = newm.capacity_;
        assert(size_ == newm.size_);
        newm.items_ = nullptr;
    }
}; // Map

class SIMap : public Map {
public:
    SIMap () {}
    Num* get(String& key) { return dynamic_cast<Num*>(get_(key)); }
    void set(String& k, Num* v) { assert(v); Map::set(k, v); }
}; // KVMap




/****************************************************************************/
class Adder : public Reader {
public:
    SIMap& map_;  // String to Num map;  Num holds an int

    Adder(SIMap& map) : map_(map)  {}

    bool visit(Row& r) override {
        String* word = r.get_string(0);
        assert(word != nullptr);
        Num* num = map_.contains(*word) ? map_.get(*word) : new Num();
        assert(num != nullptr);
        num->v++;
        map_.set(*word, num);
        return false;
    }
};

/***************************************************************************/
class Summer : public Writer {
public:
    SIMap& map_;
    size_t i = 0;
    size_t j = 0;
    size_t seen = 0;

    Summer(SIMap& map) : map_(map) {}

    void next() {
        if (i == map_.capacity_ ) return;
        if ( j < map_.items_[i].keys_.size() ) {
            j++;
            ++seen;
        } else {
            ++i;
            j = 0;
            while( i < map_.capacity_ && map_.items_[i].keys_.size() == 0 )  i++;
            if (k()) ++seen;
        }
    }

    String* k() {
        if (i==map_.capacity_ || j == map_.items_[i].keys_.size()) return nullptr;
        return (String*) (map_.items_[i].keys_.get_(j));
    }

    size_t v() {
        if (i == map_.capacity_ || j == map_.items_[i].keys_.size()) {
            assert(false); return 0;
        }
        return ((Num*)(map_.items_[i].vals_.get_(j)))->v;
    }

    void visit(Row& r) {
        if (!k()) next();
        String* key = k();
        size_t value = v();
        r.set(0, key);
        r.set(1, (int) value);
        next();
    }

    bool done() {return seen == map_.size(); }
};

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
        buf_ = *new StrBuff();
    }

    KeyBuff& c(String &s) { buf_.c(s); return *this;  }
    KeyBuff& c(size_t v) { buf_.c(v); return *this; }
    KeyBuff& c(const char* v) { buf_.c(v); return *this; }

    Key* get() {
        cout << "key buff get" << endl;
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
    }

    /** Returns a key for given node.  These keys are homed on master node
     *  which then joins them one by one. */
    Key* mk_key(size_t idx) {
        cout << "called mk_key idx = " << idx << endl;
        Key * k = kbuf.get();
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
        delete DataFrame::fromVisitor(mk_key(this_node()), &kv, "SI", cnt);
        cout << "from visitor summer done" << endl;
    }

    /** Merge the data frames of all nodes */
    void reduce() {
        if (this_node() != 0) return;
//        pln("Node 0: reducing counts...");
        cout << "Node 0: reducing counts..." << endl;
        SIMap map;
        Key* own = mk_key(0);
        merge(kv.get(*own), map);
        for (size_t i = 1; i < arg.num_nodes; ++i) { // merge other nodes
            Key* ok = mk_key(i);
            merge(kv.waitAndGet(*ok), map);
            delete ok;
        }
//        p("Different words: ").pln(map.size());
        cout << "Different words: " << map.size();

        delete own;
    }

    void merge(DataFrame* df, SIMap& m) {
        Adder add(m);
        df->map(add);
        delete df;
    }
}; // WordcountDemo