/*************************************************************************
 * StringColumn::
 * Holds string pointers. The strings are external.  Nullptr is a valid
 * value.
 */
//class Column;
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

using namespace std;

/**
 * Represent a Int of Float SoR Type
 */
class StringColumn : public Column {
public:
    String** vals_;
    size_t size_;
    size_t capacity_;

    StringColumn() {
        size_ = 0;
        capacity_ = 1000 * 1000 * 1000;
        vals_ = new String*[capacity_];
    }

    ~StringColumn(){
        delete[] vals_;
    }

    StringColumn(int n, ...) {
        va_list args;
        va_start(args, n);
        for(size_t i=0; i<n; i++)
        {
            vals_[i] = new String(va_arg(args, char*));
        }
    }

    /**
     * Returns this if it is a StringColumn
     * @return
     */
    StringColumn* as_string() {
        return this;
    }

    /**
     * Returns this if it is a IntColumn
     * @return
     */
    virtual IntColumn* as_int() {
        return nullptr;
    }

    /**
     * Returns this if it is a BoolColumn
     * @return
     */
    virtual BoolColumn*  as_bool() {
        return nullptr;
    }

    /**
     * Returns this if it is a FloatColumn
     * @return
     */
    virtual FloatColumn* as_float() {
        return nullptr;
    }

    /** Returns the string at idx; undefined on invalid idx.*/
    String* get(size_t idx) {

        return vals_[idx];
    }

    /** Out of bound idx is undefined. */
    void set(size_t idx, String* val) {
        if (idx >= 0 && idx <= this->size()) {
            vals_[idx] = val;
            size_++;
        } else {
            exit(1);
        }
    }

    /**
     * Returns the size of this StringColumn
     */
    size_t size() {
        return size_;
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
    virtual void push_back(String* val) {
       vals_[size_] = val;
       size_++;
    }

    /** Return the type of this column as a char: 'S', 'B', 'I' and 'F'. */
    virtual char get_type() {
        return 'S';
    }

    virtual String* serialize() {
        StrBuff* s = new StrBuff();
        s->c("S}");
        cout << "in string" << endl;
        cout << this->size_ << endl;
        for (int i = 0; i < this->size_; i++) {
            char str[256] = ""; /* In fact not necessary as snprintf() adds the 0-terminator. */
            snprintf(str, sizeof str, "%s}", this->vals_[i]->c_str());
            s->c(str);
        }
        s->c("!");
        return s->get();
    }
};