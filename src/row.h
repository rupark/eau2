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
    Object *elements;
    size_t size;
    size_t index;

    /** Build a row following a schema. */
    Row(Schema* scm) {
        this->elements = new Object [10];
        index = 0;
        size = scm->get_num_cols();
        for (size_t i = 0; i < scm->get_num_cols(); i++) {
            char type = scm->types->at(i);
            switch (type) {
                case 'F':
                    elements[i] = *new Float(0);
                    break;
                case 'B':
                    elements[i] = *new Bool(0);
                    break;
                case 'I':
                    elements[i] = *new Integer(0);
                    break;
                case 'S':
                    elements[i] = *new String("");
                    break;
            }
        }
    }

    ~Row() {
        for (int i = 0; i < size; i++) {
            delete elements[i];
        }
        delete[] elements;
    };

    /** Setters: set the given column with the given value. Setting a column with
      * a value of the wrong type is undefined. */
    void set(size_t col, int val) {
        if (col < size && col >= 0) {
            elements[col] = *new Integer(val);
        } else {
            exit(1);
        }
    }

    void set(size_t col, float val) {
        if (col < size && col >= 0) {
            elements[col] = *new Float(val);
        } else {
            exit(1);
        }
    }

    void set(size_t col, bool val) {
        if (col < size && col >= 0) {
            elements[col] = *new Bool(val);
        } else {
            exit(1);
        }
    }

    /** The string is external. */
    void set(size_t col, String *val) {
        if (col < size && col >= 0) {
            elements[col] = *val;
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
        return dynamic_cast<Integer &>(elements[col]).val;
    }

    bool get_bool(size_t col) {
        return dynamic_cast<Bool &>(elements[col]).val;
    }

    float get_float(size_t col) {
        return dynamic_cast<Float &>(elements[col]).val;
    }

    String *get_string(size_t col) {
        return dynamic_cast<String &>(elements[col]);
    }

    /** Number of fields in the row. */
    size_t width() {
        return size;
    }

    /** Type of the field at the given position. An idx >= width is  undefined. */
    char col_type(size_t idx) {
        if (dynamic_cast<Integer *>(this->elements[idx])) {
            return 'I';
        } else if (dynamic_cast<Bool *>(this->elements[idx])) {
            return 'B';
        } else if (dynamic_cast<Float *>(this->elements[idx])) {
            return 'F';
        } else {
            return 'S';
        }
    }

    /** Given a Fielder, visit every field of this row. The first argument is
      * index of the row in the dataframe.
      * Calling this method before the row's fields have been set is undefined. */
    void visit(size_t idx, Fielder &f) {
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

    /** Prints this Row **/
    void printRow() {
        for (int i = 0; i < this->size; i++) {
            char type = col_type(i);
            switch (type) {
                case 'F':
                    cout << this->get_float(i) << " ";
                    break;
                case 'B':
                    cout << this->get_bool(i) << " ";
                    break;
                case 'I':
                    cout << this->get_int(i) << " ";
                    break;
                case 'S':
                    cout << this->get_string(i)->c_str() << " ";
                    break;
            }
        }
        cout << " |" << endl;
    }

};