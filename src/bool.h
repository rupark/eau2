//lang CwC
#pragma once
#include <stdio.h>
#include "object.h"

/* Base Bool wrapper class. Represents the basis for all Bools. */
class Bool : public Object {
public:
    bool val;

    /**
     * Bool constructor
     * @param  bool value of this
     * @return N/A
     */
    Bool(bool value) {
        val = value;
    }

    /**
     * Bool destructor
     * @param  N/A
     * @return N/A
     */
    virtual ~Bool() {}

    /**
     * equals - compares this and other for equality
     *
     * @param Object other
     * @return bool indicating if the this and other are equal
     */
    bool equals(Object *other) {
        if ( other == nullptr) {
            return false;
        }
        Bool *s = dynamic_cast<Bool*>(other);
        if (s == nullptr) {
            return false;
        }
        if (s->val == val) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * getValue - returns this Bool as an bool
     *
     * @return bool value of this
     */
    bool getValue() {
        return val;
    }

    /**
     * hash - returns the hash value for this
     *
     * @param N/A
     * @return size_t hash value
     */
    virtual size_t hash() {

    }

    void print() override { cout << this->val << endl; }
};