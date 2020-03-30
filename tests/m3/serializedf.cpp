//
// Created by Kate Rupar on 3/30/20.
//

#include "../../src/network/serial.h"
#include "../../src/dataframe.h"

int main(int argc, char* argv[]) {

    DataFrame* d = new DataFrame(*new Schema("BFIS"));
    cout << "DF Created" << endl;
    d->columns[0]->push_back((bool)1);
    cout << "Push 0" << endl;
    d->columns[1]->push_back((float)3.0);
    cout << "Push 1" << endl;
    d->columns[2]->push_back((int)3);
    cout << "Push 2" << endl;
    d->columns[3]->push_back(new String("h"));
    cout << "Push 3" << endl;
    d->columns[0]->push_back((bool)0);
    cout << "Push 4" << endl;
    d->columns[1]->push_back((float)4.0);
    cout << "Push 5" << endl;
    d->columns[2]->push_back((int)6);
    cout << "Push 6" << endl;
    d->columns[3]->push_back(new String("f"));
    cout << "Push 7" << endl;
    Status* s = new Status(0, 0, d);
    cout << "STATUS CREATED" << endl;
    cout << s->serialize()->cstr_ << endl;

    Status* s = new Status(s.serialize()->cstr_);
    for (int i = 0; i < s->msg_->ncol; i++) {
        for (int j = 0; j < s->msg_->columns[i]->size(); j++) {
            switch (s->msg_->columns[i]->get_type()) {
                case 'F':
                    cout << s->msg_->columns[i]->as_float()->get(j);
                    break;
                case 'S':
                    cout << s->msg_->columns[i]->as_string()->get(j)->cstr_;
                    break;
                case 'B':
                    cout << s->msg_->columns[i]->as_bool()->get(j);
                    break;
                case 'I':
                    cout << s->msg_->columns[i]->as_int()->get(j);
                    break;
            }
        }
    }
};