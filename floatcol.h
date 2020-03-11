/*************************************************************************
 * floatColumn::
 * Holds float pointers. The floats are external.  Nullptr is a valid
 * value.
 */
//class Column;
class IntColumn;
class BoolColumn;
class FloatColumn;

#pragma once
#include "intcol.h"
#include "boolcol.h"
#include "stringcol.h"
#include "string.h"
#include "column.h"

/**
 * Represent a Column of Float SoR Type
 */
class FloatColumn : public Column {
public:
    float* vals_;
    size_t size_;
    size_t capacity_;

    FloatColumn() {
        size_ = 0;
        capacity_ = 1000 * 1000 * 1000;
        vals_ = new float[capacity_];
    }

    ~FloatColumn() {
        delete[] vals_;
    }

    FloatColumn(int n, ...) {
        va_list args;
        va_start(args, n);
        for(size_t i=0; i<n; i++)
        {
            vals_[i] = va_arg(args, float);
        }
    }

    /**
     * Returns this if it is a StringColumn
     * @return
     */
    StringColumn* as_string() {
        return nullptr;
    }

    /**
     * Returns this if it is a IntColumn
     * @return
     */
    IntColumn* as_int() {
        return nullptr;
    }

    /**
     * Returns this if it is a BoolColumn
     * @return
     */
    BoolColumn* as_bool() {
        return nullptr;
    }

    /**
     * Returns this if it is a FloatColumn
     * @return
     */
    FloatColumn* as_float() {
        return this;
    }

    /** Returns the float at idx; undefined on invalid idx.*/
    float * get(size_t idx) {
        if (idx >= 0 && idx <= this->size()) {
            return &vals_[idx];
        } else {
            exit(1);
        }
    }

    /** Out of bound idx is undefined. */
    void set(size_t idx, float * val) {
        if (idx >= 0 && idx <= this->size()) {
            vals_[idx] = *val;
            size_++;
        } else {
            exit(1);
        }
    }

    /**
     * Returns the size of this FloatColumn
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
        vals_[size_] = val;
        size_++;
    }

    /**
     * Adds the given String to this if it is a StringColumn
     */
    virtual void push_back(String* val) {
        exit(1);
    }

    /** Return the type of this column as a char: 'S', 'B', 'I' and 'F'. */
    char get_type() {
        return 'F';
    }
};