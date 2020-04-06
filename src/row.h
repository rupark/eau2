/*************************************************************************
 * Row::
 *
 * This class represents a single row of data constructed according to a
 * dataframe's schema. The purpose of this class is to make it easier to add
 * read/write complete rows. Internally a dataframe hold data in columns.
 * Rows have pointer equality.
 */

#pragma once

#include "float.h"
#include "bool.h"
#include "integer.h"
#include "intcol.h"
#include "boolcol.h"
#include "floatcol.h"
#include "stringcol.h"
#include "object.h"
#include "string.h"
#include "column.h"
#include "schema.h"
#include "fielder.h"
#include "iostream"

class Row : public Object {
public:
    Object** elements;
    size_t size;
    size_t index;

    /** Build a row following a schema. */
    Row(Schema& scm) {
        //cout << "copy schema" << endl;
        this->elements = new Object*[100*1000];
        index = 0;
        size = scm.ncol;
        for (size_t i = 0; i < scm.ncol; i++) {
            char type = scm.types->at(i);
            switch (type) {
                case 'F':
                    elements[size] = nullptr;
                    break;
                case 'B':
                    elements[size] = nullptr;
                    break;
                case 'I':
                    elements[size] = nullptr;
                    break;
                case 'S':
                    elements[size] = nullptr;
                    break;
            }
        }
       // cout << "schema constructed" << endl;
    }

    ~Row(){
        delete[] elements;
    }

    /** Setters: set the given column with the given value. Setting a column with
      * a value of the wrong type is undefined. */
    void set(size_t col, int val) {
        if (col <= size && col >= 0) {
            elements[col] = new Integer(val);
        } else {
            exit(1);
        }
    }

    void set(size_t col, float val) {
        if (col <= size && col >= 0) {
            elements[col] = new Float(val);
        } else {
            exit(1);
        }
    }

    void set(size_t col, bool val) {
        if (col <= size && col >= 0) {
            elements[col] = new Bool(val);
        } else {
            exit(1);
        }
    }

    /** The string is external. */
    void set(size_t col, String* val) {
        if (col <= size && col >= 0) {
            elements[col] = val;
        } else {
            exit(1);
        }
    }

    /** Set/get the index of this row (ie. its position in the dataframe. This is
     *  only used for informational purposes, unused otherwise */
    void set_idx(size_t idx) {
        index = idx;
    }

    size_t get_idx() {
        return index;
    }

    /** Getters: get the value at the given column. If the column is not
      * of the requested type, the result is undefined. */
    int get_int(size_t col) {
        int v = dynamic_cast<Integer*>(elements[col])->val;
        return v;
    }

    bool get_bool(size_t col) {
        bool v = dynamic_cast<Bool*>(elements[col])->val;
        return v;
    }

    float get_float(size_t col) {
        float v = dynamic_cast<Float*>(elements[col])->val;
        return v;
    }

    String* get_string(size_t col) {
        cout << "in get" << endl;
        String* v = dynamic_cast<String*>(elements[col]);
        cout << "cast" << endl;
        return v;
    }

    /** Number of fields in the row. */
    size_t width() {
        return size;
    }

    /** Type of the field at the given position. An idx >= width is  undefined. */
    char col_type(size_t idx) {
        if (dynamic_cast<Integer*>(this->elements[idx])) {
            return 'I';
        } else if (dynamic_cast<Bool*>(this->elements[idx])) {
            return 'B';
        } else if (dynamic_cast<Float*>(this->elements[idx])) {
            return 'F';
        } else {
            return 'S';
        }
    }

    /** Given a Fielder, visit every field of this row. The first argument is
      * index of the row in the dataframe.
      * Calling this method before the row's fields have been set is undefined. */
    void visit(size_t idx, Fielder& f) {
        for (size_t i = 0; i < this->width(); i++) {
            char type = col_type(i);
            switch (type) {
                case 'F':
                    f.accept(get_float(i));
                    break;
                case 'B':
                    f.accept(get_bool(i));
                    break;
                case 'I':
                    f.accept(get_int(i));
                    break;
                case 'S':
                    f.accept(get_string(i));
                    break;
            }
        }
    }

};