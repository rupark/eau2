/*************************************************************************
 * StringColumn::
 * Holds string pointers. The strings are external.  Nullptr is a valid
 * value.
 */

class IntColumn;

class BoolColumn;

class FloatColumn;

#pragma once

#include "column.h"
#include <cstdarg>
#include "string.h"
#include "boolcol.h"
#include "floatcol.h"
#include "stringcol.h"
#include "intcol.h"
#include <iostream>
#include <vector>

using namespace std;

/**
 * Represent a Int of Float SoR Type
 */
class StringColumn : public Column {
public:
    vector<String *> vals_;

    StringColumn() {
    }

    ~StringColumn() {
//        for (int i = 0; i < size_; i++) {
//            if (vals_[i] != nullptr) {
//                delete vals_[i];
//            }
//        }
//        delete[] vals_;
    }

//    StringColumn(int n, ...) {
//        va_list args;
//        va_start(args, n);
//        for (size_t i = 0; i < n; i++) {
//            vals_[i] = new String(va_arg(args, char * ));
//        }
//    }


    /**
    * Append missing bool is default 0.
    */
    void appendMissing() {
        this->vals_.push_back(new String(""));
    }

    /**
     * Returns this if it is a StringColumn
     * @return
     */
    StringColumn *as_string() {
        return this;
    }

    /**
     * Returns this if it is a IntColumn
     * @return
     */
    virtual IntColumn *as_int() {
        return nullptr;
    }

    /**
     * Returns this if it is a BoolColumn
     * @return
     */
    virtual BoolColumn *as_bool() {
        return nullptr;
    }

    /**
     * Returns this if it is a FloatColumn
     * @return
     */
    virtual FloatColumn *as_float() {
        return nullptr;
    }

    /** Returns the string at idx; undefined on invalid idx.*/
    String *get(size_t idx) {

        return vals_.at(idx);
    }

    /** Out of bound idx is undefined. */
    void set(size_t idx, String *val) {
        vals_.at(idx) = val;
    }

    /**
     * Returns the size of this StringColumn
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
        exit(1);
    }

    /**
     * Adds the given String to this if it is a StringColumn
     */
    virtual void push_back(String *val) {
        vals_.push_back(val);
    }

    /** Return the type of this column as a char: 'S', 'B', 'I' and 'F'. */
    virtual char get_type() {
        return 'S';
    }

    /** Returns the serialization of this StringColumn as a String */
    virtual String *serialize() {
        StrBuff *s = new StrBuff();
        s->c("S}");

        for (int i = 0; i < this->vals_.size(); i++) {
            char str[256] = ""; /* In fact not necessary as snprintf() adds the 0-terminator. */
            snprintf(str, sizeof str, "%s}", this->vals_.at(i));
            s->c(str);
        }

        s->c("!");
        return s->get();
    }
};