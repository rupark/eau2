//lang CwC
#pragma once
#include <stdio.h>
#include "object.h"

/* Base Float wrapper class. Represents the basis for all Floats.
   Only contains two basic features, equals and hash. */
class Float : public Object {
public:
    float val;

    /**
     * Float constructor
     * @param  float value
     * @return N/A
     */
    Float(float value) {
        val = value;
    }

    /**
     * Float destructor
     * @param  N/A
     * @return N/A
     */
     ~Float() {
         cout << "here" << endl;
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
        Float *s = dynamic_cast<Float*>(other);
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
     * getValue - returns this Float as an float
     *
     * @return float value of this
     */
    float getValue() {
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
};