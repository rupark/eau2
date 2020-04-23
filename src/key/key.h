//
// Created by Kate Rupar on 3/21/20.
//

#pragma once

#include "../object.h"
#include "../wrappers/string.h"
#include "../args.h"

/**
 * Represents a String, home Node association
 * To be used in a KVStore
 */
class Key : public Object {
public:
    String *name;
    size_t home;

    Key(char *name, size_t home) {
        this->name = new String(name);
        this->home = home;
    }

    Key(char *name) {
        this->name = new String(name);
        this->home = arg.index;
    }

    Key(const char *name) {
        this->name = new String(name);
        this->home = 0;
    }

    Key(Key *orig) {
        this->name = orig->name;
        this->home = orig->home;
    }

    Key(String *s) {
        this->name = s;
        this->home = 0;
    }

    ~Key() {
        cout << "in key destructor" << endl;
        if (name != nullptr) {
            delete name;
        }
    }

    /**
     * Returns true if this Key equals the given object
     */
    bool equals(Object *other) {
        if (other == this) return true;
        Key *x = dynamic_cast<Key *>(other);
        if (x == nullptr) return false;
        bool ret = name->equals(x->name);
        return ret;
    }

    /** Returns the name of this key **/
    char *c_str()  {
        StrBuff *a = new StrBuff();
        a->c(this->name->c_str());
        char *ret = a->get()->c_str();
        return ret;
    }

    Key* clone() {
        Key* key = new Key(this->name);
        return key;
    }

};
