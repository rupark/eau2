//
// Created by Kate Rupar on 4/5/20.
//

#ifndef MILESTONE1_WRITER_H
#define MILESTONE1_WRITER_H

#endif //MILESTONE1_WRITER_H

#pragma once

#include "row.h"
#include "args.h"
#include "SImap.h"

class Writer {
public:
    Writer() {

    }

    /** Writes elements to the given Row **/
    virtual void visit(Row &) {}

    /** Returns true when there are no more words to read. **/
    virtual bool done() {}
};


class FileReader : public Writer {
public:
    /** Reads next word and stores it in the row. Actually read the word.
        While reading the word, we may have to re-fill the buffer  */
    void visit(Row &r) override {
        assert(i_ < end_);
        assert(!isspace(buf_[i_]));
        size_t wStart = i_;
        while (true) {
            if (i_ == end_) {
                if (feof(file_)) {
                    ++i_;
                    break;
                }
                i_ = wStart;
                wStart = 0;
                fillBuffer_();
            }
            if (isspace(buf_[i_])) break;
            ++i_;
        }
        buf_[i_] = 0;
        String *word = new String(buf_ + wStart, i_ - wStart);
        r.set(0, word);
        ++i_;
        skipWhitespace_();
    }

    /** Returns true when there are no more words to read.  There is nothing
       more to read if we are at the end of the buffer and the file has
       all been read.     */
    bool done() override {
        return (i_ >= end_) && feof(file_);
    }

    /** Creates the reader and opens the file for reading.  */
    FileReader() {
        file_ = fopen(arg.file, "r");
        if (file_ == nullptr) cout << "Cannot open file " << arg.file << endl;
        buf_ = new char[BUFSIZE + 1]; //  null terminator
        fillBuffer_();
        skipWhitespace_();
    }

    static const size_t BUFSIZE = 1024;

    /** Reads more data from the file. */
    void fillBuffer_() {
        size_t start = 0;
        // compact unprocessed stream
        if (i_ != end_) {
            start = end_ - i_;
            memcpy(buf_, buf_ + i_, start);
        }
        // read more contents
        end_ = start + fread(buf_ + start, sizeof(char), BUFSIZE - start, file_);
        i_ = start;
    }

    /** Skips spaces.  Note that this may need to fill the buffer if the
        last character of the buffer is space itself.  */
    void skipWhitespace_() {
        while (true) {
            if (i_ == end_) {
                if (feof(file_)) return;
                fillBuffer_();
            }
            // if the current character is not whitespace, we are done
            if (!isspace(buf_[i_]))
                return;
            // otherwise skip it
            ++i_;
        }
    }

    char *buf_;
    size_t end_ = 0;
    size_t i_ = 0;
    FILE *file_;
};

class Summer : public Writer {
public:
    SIMap &map_;
    size_t i = 0;
    size_t j = 0;
    size_t seen = 0;

    Summer(SIMap &map) : map_(map) {
        cout << "map size from constructor = " << map_.size() << endl;
        cout << "welcome ===========" << map_.get(*new String("welcome"))->v << endl;
        cout << "to ===========" << map_.get(*new String("to"))->v << endl;
        cout << "the ===========" << map_.get(*new String("the"))->v << endl;
        cout << "mark ===========" << map_.get(*new String("mark"))->v << endl;
        cout << "kate ===========" << map_.get(*new String("kate"))->v << endl;
    }

    /** Progresses the map **/
    void next() {
        if (i == map_.capacity_) return;
        if (j < map_.items_[i].keys_.size()) {
            j = j + 1;
            seen = seen + 1;
        } else {
            i = i + 1;
            j = 0;
            while (i < map_.capacity_ && map_.items_[i].keys_.size() == 0) {
                i = i + 1;
            }
            if (k()) {
                seen = seen + 1;
            }
        }
    }

    /** Returns a Key name from the SIMap at the current i and j **/
    String *k() {
        if (i == map_.capacity_ || j == map_.items_[i].keys_.size()) return nullptr;
        return (String *) (map_.items_[i].keys_.get_(j));
    }

    /** Returns a value from the SIMap at the current i and j **/
    size_t v() {
        if (i == map_.capacity_ || j == map_.items_[i].keys_.size()) {
            cout << "i in case: " << i << endl;
            cout << "j in case: " << j << endl;
            cout << "map cap: "<<map_.capacity_ << endl;
            cout << "map_.items_[i].keys_.size(): "<< map_.items_[i].keys_.size() << endl;
            assert(false);
            return 0;
        }
        return ((Num *) (map_.items_[i].vals_.get_(j)))->v;
    }

    /** Gets a String, Num pair from SIMap and stores them in the given Row */
    void visit(Row &r) override {
        cout << "i: " << i << endl;
        cout << "j: " << j << endl;
        cout << "seen: " << seen << endl;
        cout << "map size: " << map_.size() << endl;
        cout << "map cap: " << map_.capacity_ << endl;
        cout << "bucket size: " << map_.items_[i].keys_.size() << endl << endl;
        if (k() == nullptr) {
            next();
            size_t value = v();
            r.set(0, key);
            r.set(1, (int) value);
            next();
        } else {
            String *key = k();
            size_t value = v();
            r.set(0, key);
            r.set(1, (int) value);
            next();
        }
    }

    /** Returns true when there are no more words String, Num pairs in SIMap */
    bool done() override { return seen >= map_.size(); }
};
