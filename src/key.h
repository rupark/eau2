//
// Created by Kate Rupar on 3/21/20.
//

#pragma once

#include "object.h"
#include "string.h"

/**
 * Represents a String, home Node association
 * To be used in a KVStore
 */
class Key : public Object {
public:
        String* name;
        size_t home;

        Key(char* name, size_t home) {
            this->name = new String(name);
            this->home = home;
        }

        // If no homenode passed in, default 0.
        Key (char* name) {
            this->name = new String(name);
            this->home = 0;
        }

        ~Key(){
            delete name;
        }

        /**
         * Returns true if this Key equals the given object
         */
        bool equals(Object* other) {
            if (other == this) return true;
            Key* x = dynamic_cast<Key *>(other);
            if (x == nullptr) return false;
            return name->equals(x->name) && home == x->home;
        }

};
