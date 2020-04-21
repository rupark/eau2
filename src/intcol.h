class StringColumn;

class BoolColumn;

class FloatColumn;

#pragma once

#include "column.h"
#include "boolcol.h"
#include "floatcol.h"
#include "stringcol.h"
#include "string.h"
#include "iostream"
#include "integer.h"
#include <vector>
#include <iostream>

using namespace std;

/**
 * Represent a Column of Integer
 */
class IntColumn : public Column {
public:
    vector<Integer *> vals_;

    IntColumn() {
    }

    ~IntColumn() {
//        cout << "in int des" << endl;
//        for (int i = 0; i < size_; i++) {
//            if (vals_[i] != nullptr) {
//                delete vals_[i];
//            }
//        }
//        delete[] vals_;
    }

    /**
    * Append missing bool is default 0.
    */
    void appendMissing() {
        vals_.push_back((int)0);
    }

    /**
     * Returns this if it is a StringColumn
     * @return
     */
    StringColumn *as_string() {
        return nullptr;
    }

    /**
     * Returns this if it is a IntColumn
     * @return
     */
    IntColumn *as_int() {
        return this;
    }

    /**
     * Returns this if it is a BoolColumn
     * @return
     */
    BoolColumn *as_bool() {
        return nullptr;
    }

    /**
     * Returns this if it is a FloatColumn
     * @return
     */
    FloatColumn *as_float() {
        return nullptr;
    }

    /** Returns the int at idx; undefined on invalid idx.*/
    int *get(size_t idx) {
        return &vals_.at(idx)->val;
    }

    /** Out of bound idx is undefined. */
    void set(size_t idx, int *val) {
        vals_.at(idx) = new Integer(*val);
    }

    /**
     * Returns the size of this IntColumn
     */
    size_t size() {
        return vals_.size();
    }

    /**
     * Adds the given int to this if it is a IntColumn
     */
    virtual void push_back(int val) {
        vals_.push_back(new Integer(val));
    }

    /**
     * Adds the given bool to this if it is a BoolColumn
     */
    virtual void push_back(bool val) {
        exit(1);
    }

    /**
     * Adds the given float to this if it is a FloatColumn
     */
    virtual void push_back(float val) {
        exit(1);
    }

    /**
     * Adds the given String to this if it is a StringColumn
     */
    virtual void push_back(String *val) {
        // if passing nullptr from <MISSING> in sor then save to array as nullptr calls this method.
        exit(1);
    }

    /** Return the type of this column as a char: 'S', 'B', 'I' and 'F'. */
    virtual char get_type() {
        return 'I';
    }

    /** Serializes this intcol **/
    virtual String *serialize() {
        StrBuff *s = new StrBuff();
        s->c("I}");

        for (int i = 0; i < this->size_; i++) {
            char str[256] = ""; /* In fact not necessary as snprintf() adds the 0-terminator. */
            snprintf(str, sizeof str, "%d}",  this->vals_.at(i)->val);
            s->c(str);
        }

        s->c("!");
        return s->get();
    }
};