//
// Created by Kate Rupar on 4/5/20.
//

#pragma once

#include "row.h"
#include "SImap.h"
//#include "set.h"

class Reader {
public:
    Reader() {

    }

    /** Reads from the given Row **/
    virtual bool visit(Row &) {}
};

class Adder : public Reader {
public:
    SIMap &map_;  // String to Num map;  Num holds an int

    Adder(SIMap &map) : map_(map) {}

    /** Reads from the given Row and adds elements to map **/
    bool visit(Row &r) override {
        // Good Debug Below.
        cout << "in visit: ";
        r.printRow();
        cout << endl;

        String *word = r.get_string(0);
        cout << "in visit word:" << word->c_str() << endl;
        assert(word != nullptr);
        Num *num = map_.contains(*word) ? map_.get(*word) : new Num();
        cout << "in visit got num" << endl;
        assert(num != nullptr);
        num->v++;
        map_.set(*word, num);
        cout << "in visit set map: word=" << word->c_str() << " num: " << num->v << endl;
        return false;
    }
};
//
///*******************************************************************************
// * A SetUpdater is a reader that gets the first column of the data frame and
// * sets the corresponding value in the given set.
// ******************************************************************************/
//class SetUpdater : public Reader {
//public:
//    Set& set_; // set to update
//
//    SetUpdater(Set& set): set_(set) {}
//
//    /** Assume a row with at least one column of type I. Assumes that there
//     * are no missing. Reads the value and sets the corresponding position.
//     * The return value is irrelevant here. */
//    bool visit(Row & row) { set_.set(row.get_int(0));  return false; }
//
//};
//
///***************************************************************************
// * The ProjectTagger is a reader that is mapped over commits, and marks all
// * of the projects to which a collaborator of Linus committed as an author.
// * The commit dataframe has the form:
// *    pid x uid x uid
// * where the pid is the identifier of a project and the uids are the
// * identifiers of the author and committer. If the author is a collaborator
// * of Linus, then the project is added to the set. If the project was
// * already tagged then it is not added to the set of newProjects.
// *************************************************************************/
//class ProjectsTagger : public Reader {
//public:
//    Set& uSet; // set of collaborator
//    Set& pSet; // set of projects of collaborators
//    Set newProjects;  // newly tagged collaborator projects
//
//    ProjectsTagger(Set& uSet, Set& pSet, DataFrame* proj):
//            uSet(uSet), pSet(pSet), newProjects(proj) {}
//
//    /** The data frame must have at least two integer columns. The newProject
//     * set keeps track of projects that were newly tagged (they will have to
//     * be communicated to other nodes). */
//    bool visit(Row & row) override {
//        int pid = row.get_int(0);
//        int uid = row.get_int(1);
//        if (uSet.test(uid))
//            if (!pSet.test(pid)) {
//                pSet.set(pid);
//                newProjects.set(pid);
//            }
//        return false;
//    }
//};
//
///***************************************************************************
// * The UserTagger is a reader that is mapped over commits, and marks all of
// * the users which commmitted to a project to which a collaborator of Linus
// * also committed as an author. The commit dataframe has the form:
// *    pid x uid x uid
// * where the pid is the idefntifier of a project and the uids are the
// * identifiers of the author and committer.
// *************************************************************************/
//class UsersTagger : public Reader {
//public:
//    Set& pSet;
//    Set& uSet;
//    Set newUsers;
//
//    UsersTagger(Set& pSet,Set& uSet, DataFrame* users):
//            pSet(pSet), uSet(uSet), newUsers(users->nrows()) { }
//
//    bool visit(Row & row) override {
//        int pid = row.get_int(0);
//        int uid = row.get_int(1);
//        if (pSet.test(pid))
//            if(!uSet.test(uid)) {
//                uSet.set(uid);
//                newUsers.set(uid);
//            }
//        return false;
//    }
//};