//lang:CwC
#pragma once
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

#include "object.h"
#include "column.h"
#include "helper.h"

#include "column_icicle.h"

#include "schema.h"
#include "dataframe.h"

// The maximum length of a line buffer. No lines over 4095 bytes
static const int buff_len = 4096;

// Reads a file and determines the schema on read
class SOR : public Object {
public:
    ColumnIce** cols_;  // owned
    Schema schema;
    size_t len_;
    size_t cap_;

    SOR() {
        len_ = 0;
        cap_ = 16;
        schema = *new Schema();

        cols_ = new ColumnIce*[cap_];

        for (size_t i = 0; i < cap_; i++) {
            cols_[i] = nullptr;
        }
    }

    ~SOR() {
        for (size_t i = 0; i < len_; i++) {
            if (cols_[i] != nullptr) {
                delete cols_[i];
            }
        }
        delete[] cols_;
    }

    // What is the type of the column at the given index? i
    // if the index is too big a -1 is returned
    ColumnType get_col_type(size_t index) {
        if (index >= len_) {
            return type_unknown;
        }
        return cols_[index]->get_type();
    }

    // What is the value for the given column index and row index?
    // If the coluumn or row index are too large a nullptr is returned
    char* get_value(size_t col_index, size_t row_index) {
        if (col_index >= len_) {
            return nullptr;
        }
        return cols_[col_index]->get_char(row_index);
    }

    // Is there a value at the given column and row index.
    // If the indexes are too large, true is returned.
    bool is_missing(size_t col_index, size_t row_index) {
        return get_value(col_index, row_index) == nullptr;
    }

    // Reads in the data from the file starting at the from byte
    // and reading at most len bytes
    void read(FILE* f, size_t from, size_t len) {
        schema = infer_columns_(f, from, len);
        schema.nrow = parse_(f, from, len); // save nrow;
    }

    // reallocates columns array if more space is needed.
    void check_reallocate_() {
        if (len_ >= cap_) {
            cap_*=2;
            ColumnIce **temp = new ColumnIce*[cap_];
            for (size_t i = 0; i < len_; i++) {
                temp[i] = cols_[i];
            }
            delete[] cols_;
            cols_ = temp;
        }
    }

    // moves the file pointer to the start of the next line.
    void seek_(FILE* f, size_t from) {
        if (from == 0) {
            fseek(f, from, SEEK_SET);
        } else {
            char buf[buff_len];
            fseek(f, from - 1, SEEK_SET);
            fgets(buf, buff_len, f);
        }
    }

    // infers and creates the column objects
    Schema infer_columns_(FILE* f, size_t from, size_t len) {
        seek_(f, from);
        char buf[buff_len];

        size_t total_bytes = 0;
        size_t row_count = 0;

        Schema* schema = new Schema();

        while (fgets(buf, buff_len, f) != nullptr && row_count < 500) {
            row_count++;
            total_bytes += strlen(buf);
            if (total_bytes >= len) {
                break;
            }
            size_t num_fields;
            char** row = parse_row_(buf, &num_fields);

            for (size_t i = 0; i < num_fields; i++) {
                check_reallocate_();
                if (i >= len_) {
                    cols_[i] = new ColumnBool();
                    len_++;
                }
                ColumnType inferred_type = infer_type(row[i]);
                if (inferred_type > cols_[i]->get_type()) {
                    delete cols_[i];

                    switch(inferred_type) {
                        case type_bool:
                            cols_[i] = new ColumnBool();
                            schema->add_column('B', new String(""));
                            break;
                        case type_int:
                            cols_[i] = new ColumnInt();
                            schema->add_column('I', new String(""));
                            break;
                        case type_float:
                            cols_[i] = new ColumnFloat();
                            schema->add_column('F', new String(""));
                            break;
                        default:
                            cols_[i] = new ColumnString();
                            schema->add_column('S', new String(""));
                            break;
                    }
                }
            }
            delete[] row;

        }

        return *schema;
    }

    // Find the start of the field value and null terminate it.
    // ASSUMPTION: input field is terminated by '>' char
    // NOTE: will mutate the field value
    // The value of len will be the offset of the null byte
    char* parse_field_(char* field, int* len) {
        char* ret = field;
        int j = 0;
        for (size_t i = 0; field[i] != '>'; i++) {
            switch (field[i]) {
                case '<':  // Malformed input
                    affirm(false, "Multiple opening <");
                case ' ': // extra space in front of field
                    ret++;
                    break;
                case '"': // the start of a String
                    ret++;
                    j = i + 1;
                    while (field[j] != '"') { // add every character until a end quote is met.
                        j++;
                    }
                    field[j] = '\0';
                    *len = j;
                    return ret;
                default:  // add every ASCII character to field return value.
                    for (j = i; field[j] != '>' && field[j] != ' '; j++);
                    field[j] = '\0';
                    *len = j;
                    return ret;
            }
        }
        *len = 0;
        return nullptr;  // missing value
    }

    // parses a row and returns a list of field values as char*
    // NOTE: will mutate the row value.
    // The value of len will be the number of fields returned
    char** parse_row_(char* row, size_t *len) {
        int cap = 16;
        int l = 0;
        char** output = new char*[cap];

        for (size_t i = 0; row[i] != '\0'; i++) {
            if (row[i] == '<') {
                int to_increment = 0;
                if (l >= cap) {
                    cap*=2;
                    char** temp = new char*[cap];
                    for (int k = 0; k < l; k++) {
                        temp[k] = output[k];
                    }
                    delete[] output;
                    output = temp;
                }

                output[l++] = parse_field_(&row[i + 1], &to_increment);
                i = i + to_increment + 1;
            }
        }

        *len = l;
        return output;
    }


    // returns numRows read.
    // read the rows from the starting byte up to len bytes into Columns.
    size_t parse_(FILE* f, size_t from, size_t len) {
        size_t numRows = 0;
        seek_(f, from);
        char buf[buff_len];

        size_t total_bytes = 0;
        while (fgets(buf, buff_len, f) != nullptr) {
            total_bytes += strlen(buf);
            if (total_bytes >= len) {
                break;
            }

            size_t num_fields;
            // current row could have more columns than infered - parse the frist len_ columns
            char** row = parse_row_(buf, &num_fields);
            // skipping rows with too few fields
            if (num_fields == 0) {
                delete[] row;
                continue;
            }

            // we skip the row as soon as we find a field that does not match our schema
            bool skip = false;
            for (size_t i = 0; i < len_; i++) {
                if (!cols_[i]->can_add(row[i])) { // TODO use schema to check type match
                    skip = true;
                    break;
                }
            }
            if (skip) {
                delete[] row;
                continue;
            }

            numRows++;

            // add all fields in this row to columns
            for (size_t i = 0; i < len_; i++) {
                if (i >= num_fields || row[i] == nullptr) {
                    cols_[i]->add_nullptr();
                } else {
                    cols_[i]->add(row[i]);
                }
            }
            delete[] row;

        }

        return numRows;
    }
};