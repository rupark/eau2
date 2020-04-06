//lang CwC
#pragma once
#include <stdio.h>
#include "object.h"

/** Base Integer wrapper class. Represents the basis for all Integers. */
class Integer : public Object {
public:
    int val;

    /**
     * Integer constructor
     * @param  int value
     * @return N/A
     */
    Integer(int value) {
        val = value;
    }

    /**
     * Integer destructor
     * @param  N/A
     * @return N/A
     */
    virtual ~Integer() {

    }

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
        Integer *s = dynamic_cast<Integer*>(other);
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
     * getValue - returns this Integer as an int
     *
     * @return int value of this
     */
    int getValue() {
        return val;
    }

    /**
     * hash - returns the hash value for this
     *
     * @param N/A
     * @return size_t hash value
     */
    virtual size_t hash() {
        return val;
    }

    void print() override { cout << this->val << endl; }
};