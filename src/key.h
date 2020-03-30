//
// Created by Kate Rupar on 3/21/20.
//

#pragma once
#include "string.h"

/**
 * Represents a String, home Node association
 * To be used in a KVStore
 */
class Key : public Object {
public:
        String* name;
        size_t homeNode;

        Key(char* name, size_t homeNode) {
            this->name = new String(name);
            this->homeNode = homeNode;
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
            return name->equals(x->name) && homeNode == x->homeNode;
        }

};
