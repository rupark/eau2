//
// Created by Kate Rupar on 4/5/20.
//

#pragma once

#include "dataframe/row.h"
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

        r.printRow();

        if (r.size == 1) {
            String *word = r.get_string(0);
            assert(word != nullptr);

            Num *num = map_.contains(*word) ? map_.get(*word) : new Num();
            assert(num != nullptr);
            num->v++;
            map_.set(*word, num);
            return false;
        } else if (r.size > 1 && r.col_type(0) == 'S' && r.col_type(1) == 'I') {

            String *word = r.get_string(0);
            int count = r.get_int(1);

            assert(word != nullptr);

            Num *num = map_.contains(*word) ? map_.get(*word) : new Num();

            assert(num != nullptr);
            num->v += count;

            map_.set(*word, num);
            return false;
        }
    }
};

