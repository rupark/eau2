//
// Created by Kate Rupar on 4/5/20.
//

#ifndef MILESTONE1_WRITER_H
#define MILESTONE1_WRITER_H

#endif //MILESTONE1_WRITER_H

#include "row.h"

class Writer {
public:
    Writer() {

    }
    virtual void visit(Row&) {}
    virtual bool done() {}
};