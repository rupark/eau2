// Lang::CwC
//
#include <assert.h>
#include <stdio.h>
#include "../src/dataframe/dataframe.h"
#include "../src/dataframe/row.h"
#include "../src/dataframe/schema.h"

#include "../src/CS4500NE/parser.h"

#include "../src/key/kvstore.h"
#include "../src/key/key.h"

#include "../src/column/column.h"
#include "../src/column/boolcol.h"
#include "../src/column/floatcol.h"
#include "../src/column/intcol.h"
#include "../src/column/stringcol.h"

#include "../src/network/serial.h"

#include "../src/wrappers/integer.h"
#include "../src/wrappers/string.h"
#include "../src/wrappers/bool.h"
#include "../src/wrappers/float.h"

#include "../src/applications/linus.h"

#include <string.h>

char* cwc_strdup(const char* src) {
    char* result = new char[strlen(src) + 1];
    strcpy(result, src);
    return result;
}

void test_stringColumn() {
    Provider::StringColumn* col = new Provider::StringColumn();
    // Basic tests
    assert(col->getType() == Provider::ColumnType::STRING);
    col->append(cwc_strdup("test1"));
    col->appendMissing();
    col->append(cwc_strdup("test2"));
    assert(col->getLength() == 3);
    assert(col->isEntryPresent(0));
    assert(!col->isEntryPresent(1));
    assert(col->isEntryPresent(2));
    assert(strcmp(col->getEntry(0), "test1") == 0);
    assert(strcmp(col->getEntry(2), "test2") == 0);

    // Use StrColumn to run a large test to make sure arraylists expand correctly
    for (size_t i = 0; i < 1024; i++) {
        col->append(cwc_strdup("test1024"));
    }

    assert(col->getLength() == 1027);
    assert(strcmp(col->getEntry(100), "test1024") == 0);
    delete col;
}

void test_intColumn() {
    Provider::IntegerColumn* col = new Provider::IntegerColumn();
    // Basic int tests
    assert(col->getType() == Provider::ColumnType::INTEGER);
    col->appendMissing();
    col->appendMissing();
    col->append(1);
    col->append(2);
    assert(col->getLength() == 4);
    assert(col->isEntryPresent(3));
    assert(!col->isEntryPresent(0));
    assert(col->getEntry(3) == 2);
    delete col;
}

void test_floatColumn() {
    Provider::FloatColumn* col = new Provider::FloatColumn();
    // Basic float tests
    assert(col->getType() == Provider::ColumnType::FLOAT);
    col->append(4500.0);
    col->appendMissing();
    col->appendMissing();
    assert(col->getLength() == 3);
    assert(col->isEntryPresent(0));
    assert(!col->isEntryPresent(2));
    assert(col->getEntry(0) == 4500.0);
    delete col;
}

void test_boolColumn() {
    Provider::BoolColumn* col = new Provider::BoolColumn();
    // Basic bool tests
    assert(col->getType() == Provider::ColumnType::BOOL);
    col->append(true);
    col->append(false);
    col->appendMissing();
    assert(col->getLength() == 3);
    assert(!col->isEntryPresent(2));
    assert(col->isEntryPresent(1));
    assert(col->getEntry(1) == false);
    delete col;
}

void test_strSlice() {
    const char* test_str = "abcde   5.2  100 test";
    StrSlice slice1{test_str, 17, 21};
    assert(slice1.getLength() == 4);
    assert(slice1.getChar(0) == 't');
    assert(slice1.getChar(1) == 'e');

    StrSlice slice2{test_str, 5, 12};
    slice2.trim(' ');
    assert(slice2.getChar(0) == '5');
    assert(slice2.getChar(2) == '2');
    char* cstr = slice2.toCString();
    assert(strcmp(cstr, "5.2") == 0);
    delete[] cstr;
    assert(slice2.toFloat() == 5.2f);

    StrSlice slice3{test_str, 13, 16};
    assert(slice3.toInt() == 100);

    StrSlice slice4{"-128", 0, 4};
    assert(slice4.toInt() == -128);

    StrSlice slice5{"+437896", 0, 5};
    assert(slice5.toInt() == 4378);
}

void test_serialization() {
    DataFrame* d = new DataFrame(*new Schema("BFIS"));
    d->columns[0]->push_back((bool)1);
    d->columns[1]->push_back((float)3.0);
    d->columns[2]->push_back((int)3);
    d->columns[3]->push_back(new String("h"));
    d->columns[0]->push_back((bool)0);
    d->columns[1]->push_back((float)4.0);
    d->columns[2]->push_back((int)6);
    d->columns[3]->push_back(new String("f"));
    Status* s = new Status(0, 0, d);
    char* serialized = s->serialize()->cstr_;
    assert(strcmp(serialized, "3?0?0?0?B}1}0}!F}3.000000}4.000000}!I}3}6}!S}h}f}!") == 0);

    Status* s2 = new Status(serialized);
    assert(0 == s2->sender_);
    assert(0 == s2->target_);
    assert(0 == s2->id_);
//    for (int i = 0; i < s2->msg_->get_num_cols(); i++) {
//        for (int j = 0; j < s2->msg_->columns[i]->size(); j++) {
//            switch (s2->msg_->columns[i]->get_type()) {
//                case 'F':
//                    cout << *s2->msg_->columns[i]->as_float()->get(j);
//                    break;
//                case 'S':
//                    cout << *s2->msg_->columns[i]->as_string()->get(j)->cstr_;
//                    break;
//                case 'B':
//                    cout << *s2->msg_->columns[i]->as_bool()->get(j);
//                    break;
//                case 'I':
//                    cout << *s2->msg_->columns[i]->as_int()->get(j);
//                    break;
//            }
//        }
//    }
}

void serial2() {

    Ack* a = new Ack(0, 1);
    Ack* g = new Ack(a->serialize()->cstr_);
    assert(strcmp(a->serialize()->cstr_,g->serialize()->cstr_) == 0);

    size_t* ports = new size_t[3];
    ports[0] = 1;
    ports[1] = 1;
    ports[2] = 1;
    String** add = new String*[3];
    add[0] = new String("127.0.0.1");
    add[1] = new String("127.0.0.2");
    add[2] = new String("127.0.0.3");
    Directory* d = new Directory(ports, add, 2);
    Directory* c = new Directory(d->serialize()->cstr_);
    assert(strcmp(d->serialize()->cstr_,c->serialize()->cstr_) == 0);

    cout << "done" << endl;
}

void testDf() {
    Schema* s = new Schema("IBSF");
    DataFrame* dataFrame = new DataFrame(*s);
    assert(dataFrame->get_num_rows() == 0);
    assert(dataFrame->get_num_cols() == 4);
    Row* r = new Row(s);
    r->set(0, (int)1);
    r->set(1, (bool)1);
    r->set(2, new String("hi"));
    r->set(3, (float)1.0);
    dataFrame->add_row(*r);
    assert(dataFrame->get_num_rows() == 1);

    IntColumn* i = new IntColumn();
    i->push_back(1);
    assert(i->size() == 1);
    dataFrame->add_column(i);
    assert(dataFrame->get_num_cols() == 5);

    DataFrame* d2 = new DataFrame(*s);
    d2->add_row(*r);
    d2->add_row(*r);
    d2->add_row(*r);
}

void testKV() {
    size_t SZ = 1000*1000;
    double* vals = new double[SZ];
    double sum = 0;
    for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
    Key key("triv",0);
    KVStore* kv = new KVStore();
    DataFrame* df = Linus::fromArray(&key, kv, SZ, vals);
    assert(df->get_double(0,1) == 1);
    DataFrame* df2 = kv.get(key);
    for (size_t i = 0; i < SZ; ++i) sum -= df2->get_double(0,i);
    assert(sum==0);
}

int main(int argc, char* argv[]) {
    (void)argc;
    (void)argv;
    printf("Running parser tests: ");
    test_stringColumn();
    test_intColumn();
    test_floatColumn();
    test_boolColumn();
    test_strSlice();
    printf("PASS\n");
    printf("Running Serialization Tests:");
    test_serialization();
    serial2();
    printf("PASS\n");
    printf("Running Dataframe Tests:");
    testDf();
    printf("PASS\n");
    printf("Running KV Tests:");
    testKV();
    printf("PASS\n");
    printf("TESTING COMPLETE\n");
    return 0;
}
