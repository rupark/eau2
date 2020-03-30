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
    String* types;
    int nrow;
    int ncol;
    String** col_names;
    String** row_names;

    /** Copying constructor */
    Schema(Schema& from) {
        this->types = new String(*from.types);
        this->col_names = new String*[1000];
        this->row_names = new String*[1000];
        this->nrow = from.nrow;
        this->ncol = from.ncol;
    }

    /** Create an empty schema **/
    Schema() {
        this->types = new String("");
        this->col_names = new String*[1000];
        this->row_names = new String*[100*1000*1000];
        this->ncol = 0;
        this->nrow = 0;
    }

    /** Create a schema from a string of types. A string that contains
      * characters other than those identifying the four type results in
      * undefined behavior. The argument is external, a nullptr argument is
      * undefined. **/
    Schema(const char* types) {
        this->types = new String(types);

        this->col_names = new String*[1000];
        this->row_names = new String*[1000];

        this->nrow = 0;
        this->ncol = this->types->size();
    }

    ~Schema() {
        delete[] types;
    }

    /** Add a column of the given type and name (can be nullptr), name
      * is external. Names are expectd to be unique, duplicates result
      * in undefined behavior. */
    void add_column(char typ, String* name) {
        this->append(typ);
        this->col_names[ncol] = name;
        ncol++;
    }

    /** Add a row with a name (possibly nullptr), name is external.  Names are
     *  expectd to be unique, duplicates result in undefined behavior. */
    void add_row(String* name) {
        this->row_names[nrow] = name;
        nrow = nrow + 1;
    }

    /** Return name of row at idx; nullptr indicates no name. An idx >= width
      * is undefined. */
    String* row_name(size_t idx) {
        return row_names[idx];
    }

    /** Return name of column at idx; nullptr indicates no name given.
      *  An idx >= width is undefined.*/
    String* col_name(size_t idx) {
        return col_names[idx];
    }

    /** Return type of column at idx. An idx >= width is undefined. */
    char col_type(size_t idx) {
        return types->at(idx);
    }

    /** Given a column name return its index, or -1. */
    int col_idx(const char* name) {
        String* nameAsString = new String(name);
        for (int i = 0; i < ncol; i++) {
            if (nameAsString->equals(col_names[i])) {
                return i;
            }
        }
        return -1;
    }

    /** Given a row name return its index, or -1. */
    int row_idx(const char* name) {
        String* nameAsString = new String(name);
        for (int i = 0; i < nrow; i++) {
            if (nameAsString->equals(row_names[i])) {
                return i;
            }
        }
        return -1;
    }

    /** The number of columns */
    size_t width() {
        return ncol;
    }

    /** The number of rows */
    size_t length() {
        return nrow;
    }

    /** Appends the given char to this Schema's types String */
    void append(char s) {
        int newsize = 1 + this->types->size();
        char* newArr = new char[newsize];
        for (int i = 0; i < this->types->size(); i++) {
            newArr[i] = this->types->at(i);
        }
        newArr[this->types->size()] = s;
        this->types = new String(newArr);
    }
};