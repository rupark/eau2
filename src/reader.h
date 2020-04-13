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
        cout << "ADDER VISIT ROW SIZE = " << r.size << endl;

        r.printRow();

        if (r.size == 1) {
            String *word = r.get_string(0);
            assert(word != nullptr);

            Num *num = map_.contains(*word) ? map_.get(*word) : new Num();
            assert(num != nullptr);
            num->v++;
            map_.set(*word, num);
            ////cout << "in visit set map: word=" << word->c_str() << " num: " << num->v << endl;
            return false;
        } else if (r.size > 1 && r.col_type(0) == 'S' && r.col_type(1) == 'I') {

            String *word = r.get_string(0);
            int count = r.get_int(1);

            assert(word != nullptr);

            Num *num = map_.contains(*word) ? map_.get(*word) : new Num();

            assert(num != nullptr);
            num->v += count;

            if (word->equals(new String("nibh"))) {
                cout << "merge nibh: " << num->v << endl;
            }

            map_.set(*word, num);
            ////cout << "in visit set map: word=" << word->c_str() << " num: " << num->v << endl;
            return false;
        }
    }
};

