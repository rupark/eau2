//
// Created by Kate Rupar on 4/5/20.
//

#pragma once

#include "row.h"
#include "SImap.h"

class Reader {
public:
    Reader() {

    }

    /** Reads from the given Row **/
    virtual bool visit(Row &) {}
};

class Adder : public Reader {
public:
    SIMap &map_;  // String to Num map;  Num holds an int

    Adder(SIMap &map) : map_(map) {}

    /** Reads from the given Row and adds elements to map **/
    bool visit(Row &r) override {
        // Good Debug Below.
        cout << "in visit: ";
        r.printRow();
        cout << endl;

        String *word = r.get_string(0);
        cout << "in visit word:" << word->c_str() << endl;
        assert(word != nullptr);
        Num *num = map_.contains(*word) ? map_.get(*word) : new Num();
        cout << "in visit got num: " << num->v << endl;
        assert(num != nullptr);
        num->v++;
        map_.set(*word, num);
        cout << "in visit set map: word=" << word->c_str() << " num: " << num->v << endl;
        return false;
    }
};

