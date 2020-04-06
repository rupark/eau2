//
// Created by Kate Rupar on 4/5/20.
//

#include "row.h"

class Reader {
public:
    Reader() {

    }
    virtual bool visit(Row&) {}
};