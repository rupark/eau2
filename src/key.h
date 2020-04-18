//
// Created by Kate Rupar on 3/21/20.
//

#pragma once

#include "object.h"
#include "string.h"
#include "args.h"

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

    // If no homenode passed in, index.
    Key(char *name) {
        this->name = new String(name);
        this->home = arg.index;
    }

    // If no homenode passed in, default 0.
    Key(const char *name) {
        this->name = new String(name);
        this->home = 0;
    }

    Key(Key *orig) {
        this->name = orig->name;
        this->home = orig->home;
    }

    Key(String *s) : Key(s->c_str()) {}

    ~Key() {
        delete name;
    }

    /**
     * Returns true if this Key equals the given object
     */
    bool equals(Object *other) {
        cout << "in key equals" << endl;
        if (other == this) return true;
        Key *x = dynamic_cast<Key *>(other);
        if (x == nullptr) return false;
        cout << "checking key equivalency: " << name->c_str() << "and " << x->name->c_str() <<endl;
        bool ret = name->equals(x->name);
        cout << "return value = " << ret << endl;
        return ret;
    }

    /** Returns the name of this key **/
    char *c_str() override {
        cout << "in cstr" << endl;
        StrBuff *a = new StrBuff();
        cout << "in cstr" << endl;
        a->c(this->name->c_str());
        cout << "added name to buf" << endl;
        char *ret = a->get()->c_str();
//        delete a;
        return ret;
    }

    Key* clone() {
        return new Key(this->name->c_str());
    }

};
