//
// Created by Kate Rupar on 4/6/20.
//

#ifndef MILESTONE1_SIMAP_H
#define MILESTONE1_SIMAP_H

#endif //MILESTONE1_SIMAP_H

#pragma once
#include "object.h"
#include "array.h"
#include "string.h"

class Num : public Object {
public:
    size_t v = 0;
    Num() {}
    Num(size_t v) : v(v) {}
};

/**  Item_ are entries in a Map, they are not exposed, are immutable, own
 *   they key, but the value is external.  author: jv */
class Items_ {
public:
    Array keys_;
    Array vals_;

    Items_() : keys_(8), vals_(8) {}

    Items_(Object *k, Object * v) : keys_(8), vals_(8) {
        keys_.push_back(k);
        vals_.push_back(v);
    }

    bool contains_(Object& k) {
        for (int i = 0; i < keys_.size(); i++)
            if (k.equals(keys_.get_(i)))
                return true;
        return false;
    }

    Object* get_(Object& k) {
        for (int i = 0; i < keys_.size(); i++)
            if (k.equals(keys_.get_(i)))
                return vals_.get_(i);
        return nullptr;
    }

    size_t set_(Object& k, Object* v) {
        for (int i = 0; i < keys_.size(); i++)
            if (k.equals(keys_.get_(i))) {
                vals_.put(i,v);
                return 0;
            }
        // The keys are owned, but the key is received as a reference, i.e. not owned so we must make a copy of it.
        keys_.push_back(k.clone());
        vals_.push_back(v);
        return 1;
    }

    size_t erase_(Object& k) {
        for (int i = 0; i < keys_.size(); i++)
            if (k.equals(keys_.get_(i))) {
                keys_.erase_(i);
                vals_.erase_(i);
                return 1;
            }
        return 0;
    }
};

/** A generic map class from Object to Object. Subclasses are responsibly of
 * making the types more specific.  author: jv */
class Map : public Object {
public:
    size_t capacity_;
    // TODO this was not size of the map, but number of occupied item positions in the top level
    size_t size_ = 0;
    Items_* items_;  // owned

    Map() : Map(10) {}
    Map(size_t cap) {
        capacity_ = cap;
        items_ = new Items_[capacity_];
    }

    ~Map() { delete[] items_; }

    /** True if the key is in the map. */
    bool contains(Object& key)  { return items_[off_(key)].contains_(key); }

    /** Return the number of elements in the map. */
    size_t size()  {
        return size_;
    }

    size_t off_(Object& k) { return  k.hash() % capacity_; }

    /** Get the value.  nullptr is allowed as a value.  */
    Object* get_(Object &key) { return items_[off_(key)].get_(key); }

    /** Add item->val_ at item->key_ either by updating an existing Item_ or
     * creating a new one if not found.  */
    void set(Object &k, Object *v) {
        if (size_ >= capacity_)
            grow();
        size_ += items_[off_(k)].set_(k,v);
    }

    /** Removes element with given key from the map.  Does nothing if the
        key is not present.  */
    void erase(Object& k) {
        size_ -= items_[off_(k)].erase_(k);
    }

    /** Resize the map, keeping all Item_s. */
    void grow() {
        //LOG("Growing map from capacity " << capacity_);
        Map newm(capacity_ * 2);
        for (size_t i = 0; i < capacity_; i++) {
            size_t sz = items_[i].keys_.size();
            for (size_t j = 0; j < sz; j++) {
                Object* k = items_[i].keys_.get_(j);
                Object* v = items_[i].vals_.get_(j);
                newm.set(*k,v);
                // otherwise the values would get deleted (if the array's destructor was doing its job I found later:)
                items_[i].vals_.put(j, nullptr);
            }
        }
        delete[] items_;
        items_ = newm.items_;
        capacity_ = newm.capacity_;
        assert(size_ == newm.size_);
        newm.items_ = nullptr;
    }
}; // Map

class SIMap : public Map {
public:
    SIMap () : Map() {}
    Num* get(String& key) { return dynamic_cast<Num*>(get_(key)); }
    void set(String& k, Num* v) { assert(v); Map::set(k, v); }
}; // KVMap