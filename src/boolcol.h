/*************************************************************************
 * BoolColumn::
 * Holds Bool pointers. The Bools are external.  Nullptr is a valid
 * value.
 */
//class Column;
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

/**
 * Represent a Column of Boolean SoR Type
 */
class BoolColumn : public Column {
public:
    Bool** vals_;
    size_t size_;
    size_t capacity_;

    BoolColumn() {
        size_ = 0;
        capacity_ = 1000 * 1000 * 1000;
        vals_ = new Bool*[capacity_];
    }

    ~BoolColumn() {
        delete[] vals_;
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
        return this;
    }

    /**
     * Returns this if it is a FloatColumn
     * @return
     */
    FloatColumn* as_float() {
        return nullptr;
    }

    /** Returns the Bool at idx; undefined on invalid idx.*/
    bool * get(size_t idx) {
        if (idx >= 0 && idx <= this->size()) {
            return &vals_[idx]->val;
        } else {
            exit(1);
        }
    }

    /** Out of bound idx is undefined. */
    void set(size_t idx, bool * val) {
        if (idx >= 0 && idx <= this->size()) {
            vals_[idx] = new Bool(*val);
            size_++;
        } else {
            exit(1);
        }
    }

    /**
     * Returns the size of this BoolColumn
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
        vals_[size_] = new Bool(val);
        size_++;
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
        // if passing nullptr from <MISSING> in sor then save to array as nullptr calls this method.
        if (val == nullptr) {
            this->vals_[size_] = nullptr;
            size_++;
        } else {
            exit(1);
        }
    }

    /** Return the type of this column as a char: 'S', 'B', 'I' and 'F'. */
    virtual char get_type() {
        return 'B';
    }

    virtual String* serialize() {
        StrBuff* s = new StrBuff();
        result->c("B}");
        for (int i = 0; i < this->size_; i++) {
            char str[256] = ""; /* In fact not necessary as snprintf() adds the 0-terminator. */
            snprintf(str, sizeof str, "%d}", this->vals_[i]->val);
            s->c(str);
        }
        result->c("!");
        return result->get();
    }
};