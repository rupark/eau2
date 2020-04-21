class StringColumn;

class IntColumn;

class FloatColumn;

#pragma once

#include "column.h"
#include "intcol.h"
#include "floatcol.h"
#include "stringcol.h"
#include "string.h"
#include "bool.h"
#include <iostream>
#include <vector>
#include <bits/stdc++.h>

using namespace std;

/**
 * Represent a Column of Bool
 */
class BoolColumn : public Column {
public:
    vector<Bool *> vals_;

    BoolColumn() {
    }

    ~BoolColumn() {
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
        vals_.push_back(false);
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
        return nullptr;
    }

    /**
     * Returns this if it is a BoolColumn
     * @return
     */
    BoolColumn *as_bool() {
        return this;
    }

    /**
     * Returns this if it is a FloatColumn
     * @return
     */
    FloatColumn *as_float() {
        return nullptr;
    }

    /** Returns the Bool at idx; undefined on invalid idx.*/
    bool *get(size_t idx) {
        return &vals_.at(idx)->val;
    }

    /** Out of bound idx is undefined. */
    void set(size_t idx, bool *val) {
        vals_.at(idx) = new Bool(*val);
    }

    /**
     * Returns the size of this BoolColumn
     */
    size_t size() {
        return vals_.size();
    }

    /**
     * Adds the given int to this if it is a IntColumn
     */
    virtual void push_back(int val) {
        exit(1);
    }

    /**
     * Adds the given bool to this if it is a BoolColumn
     */
    virtual void push_back(bool val) {
        vals_.push_back(new Bool(val));
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
        exit(1);

    }

    /** Return the type of this column as a char: 'S', 'B', 'I' and 'F'. */
    virtual char get_type() {
        return 'B';
    }

    /** Serializes this BoolCol **/
    virtual String *serialize() {
        StrBuff *s = new StrBuff();
        s->c("B}");

        for (int i = 0; i < this->vals_.size(); i++) {
            char str[256] = ""; /* In fact not necessary as snprintf() adds the 0-terminator. */
            snprintf(str, sizeof str, "%d}", this->vals_.at(i)->val);
            s->c(str);
        }

        s->c("!");
        return s->get();
    }
};