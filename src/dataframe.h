/****************************************************************************
 * DataFrame::
 *
 * A DataFrame is table composed of columns of equal length. Each column
 * holds values of the same type (I, S, B, F). A dataframe has a schema that
 * describes it.
 */
#pragma once

#include "floatcol.h"
#include "intcol.h"
#include "boolcol.h"
#include "stringcol.h"
#include "column.h"
#include "string.h"
#include "fielder.h"
#include "schema.h"
#include "row.h"
#include "rower.h"
#include <iostream>
#include <thread>
#include "key.h"
#include "kvstore.h"
#include "reader.h"
#include "column_prov.h"
#include "writer.h"
#include "reader.h"

using namespace std;

/** Represents a set of data */
class DataFrame : public Object {
public:
    Schema schema;
    Column **columns;
    int nrow;
    int ncol;

    /** Create a data frame with the same columns as the given df but with no rows or rownmaes */
    DataFrame(DataFrame &df) {
        int ncol = df.ncols();
        int nrow = df.nrows();
        schema = df.get_schema();
        columns = df.columns;
    }

    ~DataFrame() {
        for (int i = 0; i < ncol; i++) {
            delete columns[i];
        }
    }

    /** Create a data frame from a schema. All columns are created empty. */
    DataFrame(Schema &schema) {
        this->columns = new Column *[100 * 1000 * 1000];

        ncol = schema.width();

        this->schema = *new Schema(schema);

        nrow = 0;
        // set nrow to 0 but if schema comes in with nrow then use that
        nrow = schema.nrow;

        for (size_t i = 0; i < schema.ncol; i++) {
            char type = this->schema.col_type(i);
            switch (type) {
                case 'F':
                    columns[i] = new FloatColumn();
                    break;
                case 'B':
                    columns[i] = new BoolColumn();
                    break;
                case 'I':
                    columns[i] = new IntColumn();
                    break;
                case 'S':
                    columns[i] = new StringColumn();
                    break;
            }
        }
    }

    /** Fill DataFrame from group 4500NE's sorer adapter */
    DataFrame(Provider::ColumnSet *data, size_t num_columns) {
        this->columns = new Column *[100 * 1000 * 1000];
        this->schema = *new Schema();

        Column *working_col;

        // create columns and fill this df
        for (size_t i = 0; i < num_columns; i++) {
            // determine col type
            switch (data->getColumn(i)->getType()) {

                case Provider::ColumnType::BOOL :

                    working_col = new BoolColumn();
                    break;
                case Provider::ColumnType::FLOAT :

                    working_col = new FloatColumn();
                    break;
                case Provider::ColumnType::INTEGER :
                    working_col = new IntColumn();
                    break;
                case Provider::ColumnType::STRING :
                    working_col = new StringColumn();
                    break;
                case Provider::ColumnType::UNKNOWN :
                    exit(-1);
                    break;
            }
            // fill the specific typed column
            for (size_t j = 0; j < data->getColumn(i)->getLength(); j++) {
                switch (data->getColumn(i)->getType()) {
                    case Provider::ColumnType::BOOL :
                        if (checkColumnEntry(data->getColumn(i), j)) {
                            if (data->getColumn(i)->isEntryPresent(j)) {
                                working_col->push_back(
                                        dynamic_cast<Provider::BoolColumn *>(data->getColumn(i))->getEntry(j));
                            } else {
                                working_col->push_back(nullptr);
                            }
                        }
                        break;
                    case Provider::ColumnType::FLOAT :
                        if (checkColumnEntry(data->getColumn(i), j)) {
                            if (data->getColumn(i)->isEntryPresent(j)) {
                                working_col->push_back(
                                        dynamic_cast<Provider::FloatColumn *>(data->getColumn(i))->getEntry(j));
                            } else {
                                working_col->push_back(nullptr);
                            }
                        }
                        break;
                    case Provider::ColumnType::INTEGER :
                        if (checkColumnEntry(data->getColumn(i), j)) {
                            if (data->getColumn(i)->isEntryPresent(j)) {
                                working_col->push_back(
                                        dynamic_cast<Provider::IntegerColumn *>(data->getColumn(i))->getEntry(j));
                            } else {
                                working_col->push_back(nullptr);
                            }
                        }
                        break;
                    case Provider::ColumnType::STRING :
                        if (checkColumnEntry(data->getColumn(i), j)) {
                            if (data->getColumn(i)->isEntryPresent(j)) {
                                working_col->push_back(new String(
                                        dynamic_cast<Provider::StringColumn *>(data->getColumn(i))->getEntry(j)));
                            } else {
                                working_col->push_back(nullptr);
                            }
                        }
                        break;
                    case Provider::ColumnType::UNKNOWN:

                        break;
                }
            }

            // add this column to this dataframe
            this->add_column(working_col, nullptr);
        }

    }


    /**
     * Terminates if the given column is not large enough to have the given entry index.
     * @param col The column
     * @param which The entry index
     */
    bool checkColumnEntry(Provider::BaseColumn *col, size_t which) {
        if (which >= col->getLength()) {
            return false;
        }
        return true;
    }

    /** Returns the dataframe's schema. Modifying the schema after a dataframe
      * has been created in undefined. */
    Schema &get_schema() {
        return schema;
    }

    /** Adds a column this dataframe, updates the schema, the new column
      * is external, and appears as the last column of the dataframe, the
      * name is optional and external. A nullptr colum is undefined. */
    void add_column(Column *col, String *name) {
        if (col == nullptr) {
            exit(1);
        } else {
            columns[ncol] = col;
            schema.add_column(col->get_type(), name);
            ncol++;
        }
    }

    /** Return the value at the given column and row. Accessing rows or
     *  columns out of bounds, or request the wrong type is undefined.*/
    int get_int(size_t col, size_t row) {
        return *columns[col]->as_int()->get(row);
    }

    bool get_bool(size_t col, size_t row) {
        return *columns[col]->as_bool()->get(row);
    }

    float get_float(size_t col, size_t row) {
        return *columns[col]->as_float()->get(row);
    }

    String *get_string(size_t col, size_t row) {
        return columns[col]->as_string()->get(row);
    }

    /** Return the offset of the given column name or -1 if no such col. */
    int get_col(String &col) {
        return schema.col_idx(col.c_str());
    }

    /** Return the offset of the given row name or -1 if no such row. */
    int get_row(String &col) {
        return schema.row_idx(col.c_str());
    }

    /** Set the value at the given column and row to the given value.
      * If the column is not  of the right type or the indices are out of
      * bound, the result is undefined. */
    void set(size_t col, size_t row, int val) {
        columns[col]->as_int()->set(row, &val);
    }

    void set(size_t col, size_t row, bool val) {
        columns[col]->as_bool()->set(row, &val);
    }

    void set(size_t col, size_t row, float val) {
        columns[col]->as_float()->set(row, &val);
    }

    void set(size_t col, size_t row, String *val) {
        columns[col]->as_string()->set(row, val);
    }

    /** Set the fields of the given row object with values from the columns at
      * the given offset.  If the row is not form the same schema as the
      * dataframe, results are undefined.
      */
    void fill_row(size_t idx, Row &row) {
        for (size_t i = 0; i < ncol; i++) {
            switch (columns[i]->get_type()) {
                case 'F':
                    row.set(i, columns[i]->as_float()->get(idx));
                    break;
                case 'B':
                    row.set(i, columns[i]->as_bool()->get(idx));
                    break;
                case 'I':
                    row.set(i, columns[i]->as_int()->get(idx));
                    break;
                case 'S':
                    row.set(i, columns[i]->as_string()->get(idx));
                    break;
            }
        }
    }

    /** Add a row at the end of this dataframe. The row is expected to have
     *  the right schema and be filled with values, otherwise undedined.  */
    void add_row(Row &row) {
        row.set_idx(nrow);
        this->nrow++;
        schema.nrow++;
        for (size_t i = 0; i < ncol; i++) {
            switch (columns[i]->get_type()) {
                case 'F':
                    columns[i]->push_back(row.get_float(i));
                    break;
                case 'B':
                    columns[i]->push_back(row.get_bool(i));
                    break;
                case 'I':
                    this->columns[i]->push_back(row.get_int(i));
                    break;
                case 'S':
                    columns[i]->push_back(row.get_string(i));
                    break;
            }
        }
    }

    /** The number of rows in the dataframe. */
    size_t nrows() {
        return schema.length();
    }

    /** The number of columns in the dataframe.*/
    size_t ncols() {
        return schema.width();
    }

    /** Visit rows in order */
    void map(Rower &r) {
        for (size_t i = 0; i < this->nrows(); i++) {
            Row *row = new Row(this->schema);
            for (size_t j = 0; j < this->ncols(); j++) {
                switch (row->col_type(j)) {
                    case 'I':
                        row->set(j, this->columns[j]->as_int()->get(i));
                        break;
                    case 'B':
                        row->set(j, this->columns[j]->as_bool()->get(i));
                        break;
                    case 'S':
                        row->set(j, this->columns[j]->as_string()->get(i));
                        break;
                    case 'F':
                        row->set(j, this->columns[j]->as_float()->get(i));
                        break;
                }
            }
            r.accept(*row);
        }
    }

    /** Visits the rows in order on THIS node */
    void local_map(Adder &r) {
        for (size_t i = 0; i < this->nrows(); i++) {
            Row *row = new Row(this->schema);
            for (size_t j = 0; j < this->ncols(); j++) {
                switch (row->col_type(j)) {
                    case 'I':
                        row->set(j, this->columns[j]->as_int()->get(i));
                        break;
                    case 'B':
                        row->set(j, this->columns[j]->as_bool()->get(i));
                        break;
                    case 'S':
                        row->set(j, this->columns[j]->as_string()->get(i));
                        break;
                    case 'F':
                        row->set(j, this->columns[j]->as_float()->get(i));
                        break;
                }
            }
            r.visit(*row);
        }
    }

    /** Applies the given Adder to each Row in this DataFrame **/
    void map(Adder &r) {
        local_map(r);
    }

    /** Create a new dataframe, constructed from rows for which the given Rower
      * returned true from its accept method. */
    DataFrame *filter(Rower &r) {
        DataFrame *d = new DataFrame(this->get_schema());
        for (size_t i = 0; i < this->nrows(); i++) {
            Row *row = new Row(this->schema);
            for (size_t j = 0; j < this->ncols(); j++) {
                switch (row->col_type(j)) {
                    case 'I':
                        row->set(j, this->columns[j]->as_int()->get(i));
                        break;
                    case 'B':
                        row->set(j, this->columns[j]->as_bool()->get(i));
                        break;
                    case 'S':
                        row->set(j, this->columns[j]->as_string()->get(i));
                        break;
                    case 'F':
                        row->set(j, this->columns[j]->as_float()->get(i));
                        break;
                }
            }
            if (r.accept(*row)) {
                d->add_row(*row);
            }
        }
        return d;
    }

    /** Print the dataframe in SoR format to standard output. */
    void print() {
        for (size_t i = 0; i < nrow; i++) {
            for (size_t j = 0; j < ncol; j++) {
                switch (columns[j]->get_type()) {
                    case 'F':
                        cout << "<" << columns[j]->as_float()->get(i) << ">";
                        break;
                    case 'B':
                        cout << "<" << columns[j]->as_bool()->get(i) << ">";
                        break;
                    case 'I':
                        cout << "<" << columns[j]->as_int()->get(i) << ">";
                        break;
                    case 'S':
                        cout << "<" << columns[j]->as_string()->get(i)->cstr_ << ">";
                        break;
                }
            }
            cout << endl;
        }
    }

    /**
     * Contructs a DataFrame from the given array of doubles and associates the given Key with the DataFrame in the given KVStore
     */
    static DataFrame *fromArray(Key *key, KVStore *kv, size_t sz, double *vals) {
        DataFrame *df = new DataFrame(*new Schema("F"));
        for (int i = 0; i < sz; i++) {
            df->columns[0]->push_back((float) vals[i]);
        }
        kv->put(key, df);
        return df;
    }

    /**
     * Contructs a DataFrame of the given schema from the given FileReader and puts it in the KVStore at the given Key
     */
    static DataFrame *fromVisitor(Key *key, KVStore *kv, char *schema, FileReader w) {
        DataFrame *df = new DataFrame(*new Schema(schema));
        while (!w.done()) {
            Row *r = new Row(*new Schema(schema));
            w.visit(*r);
            df->add_row(*r);
            cout << "ROW: " << r->get_string(0)->c_str() << endl;
        }
        kv->put(key, df);
        return df;
    }

    /** Returns a section of this DataFrame as a new DataFrame **/
    DataFrame *chunk(size_t chunk) {
        int start_row = chunk * arg.rows_per_chunk;
        DataFrame *df = new DataFrame(this->schema);
        for (size_t i = start_row; start_row < start_row + arg.rows_per_chunk; i++) {
            if (i >= nrow) {
                return df;
            } else {
                Row *r = new Row(this->schema);
                this->fill_row(i, *r);
            }
        }
        return df;
    };

    /**
     * Contructs a DataFrame of the given schema from the given Summer and puts it in the KVStore at the given Key
     */
    static DataFrame *fromVisitor(Key *key, KVStore *kv, char *schema, Summer w) {
        DataFrame *df = new DataFrame(*new Schema(schema));
        while (!w.done()) {
            Row *r = new Row(*new Schema(schema));
            w.visit(*r)
            df->add_row(*r);
        }
        kv->put(key, df);
        return df;
    }

    /**
     * Returns the double at the given column and row in this DataFrame
     */
    float get_double(int col, int row) {
        return *this->columns[col]->as_float()->get(row);
    }

    /**
     * Contructs a DataFrame from the size_t and associates the given Key with the DataFrame in the given KVStore
     */
    static DataFrame *fromScalar(Key *key, KVStore *kv, size_t scalar) {
        DataFrame *df = new DataFrame(*new Schema("I"));
        df->columns[0]->push_back((int) scalar);
        kv->put(key, df);
        return df;
    }


};