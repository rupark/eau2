class IntColumn;

class BoolColumn;

class FloatColumn;

#pragma once

#include "intcol.h"
#include "boolcol.h"
#include "stringcol.h"
#include "../wrappers/string.h"
#include "column.h"
#include "../wrappers/float.h"
#include <iostream>
#include <string>
#include <vector>

using namespace std;

/**
 * Represent a Column of Float
 */
class FloatColumn : public Column {
public:
    vector<Float*> vals_;

    FloatColumn() {
    }

    ~FloatColumn() {
        for (int i = 0; i < size_; i++) {
            if (vals_[i] != nullptr) {
                delete vals_[i];
            }
        }
//        delete vals_;
    }

    /**
    * Append missing bool is default 0.
    */
    void appendMissing() {
        vals_.push_back((float)0);
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
        return nullptr;
    }

    /**
     * Returns this if it is a FloatColumn
     * @return
     */
    FloatColumn *as_float() {
        return this;
    }

    /** Returns the float at idx; undefined on invalid idx.*/
    float *get(size_t idx) {
            return &vals_[idx]->val;
    }

    /** Out of bound idx is undefined. */
    void set(size_t idx, float *val) {
            vals_[idx] = new Float(*val);
    }

    /**
     * Returns the size of this FloatColumn
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
        exit(1);
    }


    /**
     * Adds the given float to this if it is a FloatColumn
     */
    virtual void push_back(float val) {
        vals_.push_back(new Float(val));
    }

    /**
     * Adds the given String to this if it is a StringColumn
     */
    virtual void push_back(String *val) {
            exit(1);
    }

    /** Return the type of this column as a char: 'S', 'B', 'I' and 'F'. */
    virtual char get_type() {
        return 'F';
    }

    /** Serializes this FloatColumn **/
    virtual String *serialize() {
        StrBuff *s = new StrBuff();
        s->c("F}");

        for (int i = 0; i < this->vals_.size(); i++) {
            char str[256] = ""; /* In fact not necessary as snprintf() adds the 0-terminator. */
            snprintf(str, sizeof str, "%f}", this->vals_[i]->val);
            s->c(str);
        }

        s->c("!");
        return s->get();
    }
};