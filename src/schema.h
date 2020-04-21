/*************************************************************************
 * Schema::
 * A schema is a description of the contents of a data frame, the schema
 * knows the number of columns and number of rows, the type of each column,
 * optionally columns and rows can be named by strings.
 * The valid types are represented by the chars 'S', 'B', 'I' and 'F'.
 */

#pragma once

#include "intcol.h"
#include "boolcol.h"
#include "floatcol.h"
#include "stringcol.h"
#include "string.h"
#include <string>

class Schema : public Object {
public:
    String *types;
    size_t ncol;
    size_t nrow;

    /** Copying constructor */
    Schema(Schema &from) {
        this->types = new String(*from.types);
        this->ncol = from.ncol;
        this->nrow = from.nrow;
    }

    /** Create an empty schema **/
    Schema() {
        this->types = new String("");
        this->ncol = 0;
        this->nrow = 0;
    }

    /** Create a schema from a string of types. A string that contains
      * characters other than those identifying the four type results in
      * undefined behavior. The argument is external, a nullptr argument is
      * undefined. **/
    Schema(const char *types) {
        this->types = new String(types);
        this->ncol = this->types->size();
        this->nrow = 0;
    }

    ~Schema() {
        delete types;
    }

    /** Add a column of the given type and name (can be nullptr), name
      * is external. Names are expectd to be unique, duplicates result
      * in undefined behavior. */
    void add_column(char typ) {
        this->append(typ);
        ncol++;
    }

    void add_row() {
        nrow++;
    }

    /** Return type of column at idx. An idx >= width is undefined. */
    char col_type(size_t idx) {
        return types->at(idx);
    }

    size_t get_num_rows() {
        return nrow;
    }

    /** The number of columns */
    size_t get_num_cols() {
        return ncol;
    }

    /** Appends the given char to this Schema's types String */
    void append(char s) {
        int newsize = 1 + this->types->size();
        char *newArr = new char[newsize];
        for (int i = 0; i < this->types->size(); i++) {
            newArr[i] = this->types->at(i);
        }
        newArr[this->types->size()] = s;
        this->types = new String(newArr);
    }
};