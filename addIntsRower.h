//
// Created by Kate Rupar on 2/14/20.
//

#pragma once
#include "column.h"
#include "boolcol.h"
#include "floatcol.h"
#include "stringcol.h"
#include "string.h"
#include "iostream"
#include "row.h"
#include "rower.h"
#include "intcol.h"

/**
 * A Rower with a purpose of converting every int to 4
 */
class addIntsRower : public Rower {
public:

    addIntsRower() {
    }

    ~addIntsRower() {
    }

    //If the col_type is int, sets the value to 4
    //Returns true if the row contains an IntColumn
     bool accept(Row& r) {
        bool intFlag = false;
        for (int i = 0; i < r.width(); i++) {
            if (r.col_type(i) == 'I') {
                r.set(0, 4);
                intFlag = true;
            }
        }
        return intFlag;
    }

    /** Return a copy of the object; nullptr is considered an error */
     Rower* clone() {
        return new addIntsRower();
    }

    /** Once traversal of the data frame is complete the rowers that were
        split off will be joined.  There will be one join per split. The
        original object will be the last to be called join on. The join method
        is reponsible for cleaning up memory. */
     void join_delete(Rower* other) {
        delete other;
    }
};