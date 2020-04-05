//
// Created by Mark Thekkethala on 1/14/2020.
//

#pragma once

#include "string.h"

// Size must be positive
// 0 starting index
// grows when only one spot left by doubling size of array
// shrinks when less than half of array is in use by shrinking to half size to free up memory
// needs to use a capacity field also to keep track of how much memory is allocated
// Design: StrList starts at size 0
// Design capacity starts at 0
// Design size means how many actual filled elements
class Array : public Object {
public:
    size_t size_;
    Object** arr_;
    size_t hash_;
    size_t capacity_; // amount of memory allocated

    Array (int capacity) {
        size_ = 0;
        capacity_ = capacity;
        arr_ = new Object*[capacity_];
        hash_ = hash_me();
    }

    ~Array() {
        for (int i = 0; i < capacity_; i++) {
            delete arr_[i];
        }
        delete[] arr_;
    }

    // Return the number of elements in the collection
    size_t size() {
        return size_;
    }

    // Returns the element at index
    Object*  get(size_t index) {
        if (index >= 0 && index < size_) {
            if (arr_[index] == nullptr) {
                exit(-1); // null pointer at spot
            } else {
                return dynamic_cast<Object*>(arr_[index]); // needs to return pointer to the Object
            }
        } else {
            exit(-1); // error if index requested is not in the range of the list
        }
    }

    // Removes all of elements from this list
    void clear() {
        size_ = 0;
        arr_ = new Object*[size_];
    }

    // Returns the hash code value for this list.
    size_t hash() {
        size_t hash_calc = 0;
        for (size_t i = 0; i < size_; i++) {
            hash_calc = hash_calc + arr_[i]->hash();
        }
//            if (hash_ == 0) {
//                hash_ = hash_me(); // use Object class to calculate hash
//            }
        return hash_calc;
    }

    // Inserts all of elements in c into this list at i
    void add_all(size_t i, Array* c) {
        for (size_t j = 0; j < c->size(); j++) {
            add(i+j,c->get(j));
        }
    }

    // Compares o with this list for equality
    bool equals(Object* o) {
        if (o == nullptr) {
            return false;
        }
        Array* other = dynamic_cast<Array*>(o);
        if (other == nullptr) {
            return false;
        }
        // other is Array from this point
        // check sizes
        if (other->size() != size_) {
            return false;
        }
        // check for equality of elements
        for (size_t i = 0; i < size_; i++) {
            // if equality not found return false
            if (!arr_[i]->equals(other->get(i))) {
                return false;
            }
        }
        return true;
    }

    // Returns the index of the first occurrence of o, or -1 if not there
    size_t index_of(Object* o) {
        if (o == nullptr) {
            return -1;
        }
        Object* other = dynamic_cast<Object*>(o);
        if (other == nullptr) {
            return -1;
        }
        // run through arr_ and find other
        for (size_t i = 0; i < size_; i++) {
            if (arr_[i]->equals(other)) {
                return i;
            }
        }
        return -1;
    }

    // Replaces the element at i with e
    Object* set(size_t i, Object* e) {
        if (i < 0 || i >= size_) {
            exit(-1); // out of bounds
        }
        Object* returnVal = arr_[i];
        arr_[i] = e;
        return returnVal;
    }

    // grows capacity by doubling
    void grow() {
        capacity_ *= 2;
        // create new temp array
        Object** arr_temp = new Object*[capacity_];
        // fill temp array
        for (int i = 0; i < size_; i++) {
            arr_temp[i] = arr_[i];
        }

        // free array STILL NEEDS TO DESTRUCT
        //delete[] arr_;

        // save new array
        arr_ = arr_temp;

        // free temp array
        //delete[] arr_temp;
    }

    // ONLY THESE METHODS CAN AFFECT SIZE OF ARR_
    // Appends e to end
    void push_back(Object* e) {
        // check if there is free space in arr_
        // if size is less than capacity there is at least one open spot
        if (size_ < capacity_) {
            // there is space in array so add element
            arr_[size_] = e;
            size_++;
        } else {
            // there is no space in array so reallocate
            // double capacity
            grow();
            arr_[size_] = e;
            size_++;
        }
    }

    // Inserts e at i
    void add(size_t i, Object* e) {


        // check if theres space
        if (size_ >= capacity_) {
            grow();
            std::cout << "Growing Array: Capacity New - " << capacity_ << endl;
            //print_list();
        }

        Object** temp = new Object*[capacity_];

        // check index
        if (i >= 0 && i < size_) {
            // shift array right
            size_t newPos;

            for (size_t j=0; j<size_; j++) {
                // do the shift if j == i
                if (j == i) {
                    newPos = j + 1;
                } else {
                    newPos = j;
                }
                temp[newPos] = arr_[j];
            }

            // add element e at i
            temp[i] = e;
            // increase size_
            size_++;
            // switch the temp and arr
            delete[] arr_; // need to free each element
            arr_ = temp;
        }

    }

    // Removes the element at i
    Object* remove(size_t i) {
        Object* returnVal = nullptr;
        Object** temp = new Object*[capacity_];
        // check index
        if (i >= 0 && i < size_) {
            returnVal = dynamic_cast<Object*>(arr_[i]); // needs to return a pointer

            // shift array left
            size_t newPos;

            for (size_t j = 0; j < size_; j++) {
                // do the shift if j == i
                if (j != i) {
                    newPos++;
                }
                temp[newPos] = arr_[j];
            }

            // decrease size_
            size_--;
            // switch the temp and arr
            delete[] arr_; // need to free each element
            arr_ = temp;
        }
        return returnVal;
    }




};