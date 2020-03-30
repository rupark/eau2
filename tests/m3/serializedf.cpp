//
// Created by Kate Rupar on 3/30/20.
//

#include "../../src/network/serial.h"
#include "../../src/dataframe.h"

int main(int argc, char* argv[]) {

    DataFrame* d = new DataFrame(*new Schema("BFIS"));
    d->columns[0]->push_back((int)1);
    d->columns[1]->push_back((float)3.0);
    d->columns[2]->push_back((int)3);
    d->columns[3]->push_back(new String("h"));
    d->columns[0]->push_back((int)0);
    d->columns[1]->push_back((float)4.0);
    d->columns[2]->push_back((int)6);
    d->columns[3]->push_back(new String("f"));
    Status* s = new Status(0, 0, d);
    cout << s->serialize()->cstr_ << endl;

}