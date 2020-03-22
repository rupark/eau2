//
// Created by Mark Thekkethala on 1/14/2020.
//

#ifndef LIST_H
#define LIST_H

#endif //LIST_H

#pragma once

#include "string.h"
#include "Column.h"
#include "column.h"

// Size must be positive
// 0 starting index
// grows when only one spot left by doubling size of array
// shrinks when less than half of array is in use by shrinking to half size to free up memory
// needs to use a capacity field also to keep track of how much memory is allocated
// Design: List starts at size 0
// Design capacity starts at 0
// Design size means how many actual filled elements
class List : public Column {
    public:
        size_t size_;
        Column** arr_;
        size_t capacity_; // amount of memory allocated

        List () {
            size_ = 0;
            capacity_ = 2;
            arr_ = new Column*[capacity_];
        }

        ~List() {
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
        Column*  get(size_t index) {
            if (index >= 0 && index < size_) {
                if (arr_[index] == nullptr) {
                    std::cerr << "error" << std::endl;
                    exit(-1); // null pointer at spot
                } else {
                    return arr_[index]; // needs to return pointer to the String
                }
            } else {
                exit(-1); // error if index requested is not in the range of the list
            }
        }

        // Removes all of elements from this list
        void clear() {
            size_ = 0;
            arr_ = new Column*[size_];
        }

        // Inserts all of elements in c into this list at i
        void add_all(size_t i, List* c) {
            for (size_t j = 0; j < c->size(); j++) {
                add(i+j,c->get(j));
            }
        }

        // Compares o with this list for equality
        bool equals(Column* o) {
            if (o == nullptr) {
             return false;
            }
            List* other = dynamic_cast<List*>(o);
            if (other == nullptr) {
                return false;
            }
            // other is List from this point
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
        size_t index_of(Column* o) {
            if (o == nullptr) {
                return -1;
            }
            Column* other = dynamic_cast<Column*>(o);
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
        Column* set(size_t i, Column* e) {
            if (i < 0) {
                exit(-1); // out of bounds
            } else if (i>= size_) {
                grow();
            }
            Column* returnVal = arr_[i];
            arr_[i] = e;
            return returnVal;
        }

        // grows capacity by doubling
        void grow() {
            capacity_ *= 2;
            // create new temp array
            Column** arr_temp = new Column*[capacity_];
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
        void push_back(Column* e) {
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
        void add(size_t i, Column* e) {


            // check if theres space
            if (size_ >= capacity_) {
                grow();
                std::cout << "Growing Array: Capacity New - " << capacity_ << endl;
            }

            Column** temp = new Column*[capacity_];

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
        Column* remove(size_t i) {
            Column* returnVal = nullptr;
            Column** temp = new Column*[capacity_];
            // check index
            if (i >= 0 && i < size_) {
                returnVal = dynamic_cast<Column*>(arr_[i]); // needs to return a pointer

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
//
//class SortedList : public List {
//	public:
//        // adds the element in correctly sorted list spot
//        void sorted_add(String* e) {
//            for (int j=0; j < size_; j++) {
//                if (strcmp(e->val_, arr_[j]->val_) < 0) {
//                    std::cout << "Add - j: " << j << " e:" <<  e->val_ << endl;
//                    add(j, e);
//                    return;
//                }
//            }
//            push_back(e);
//        }
//};
