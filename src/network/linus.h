//
// Created by Kate Rupar on 4/9/20.
#pragma once
#include "application.h"
#include "../dataframe.h"
#include "../key.h"
#include "../reader.h"
#include "../SImap.h"
#include "../writer.h"
#include "network.h"
#include <iostream>
#include "../parser.h"
using namespace std;

/*************************************************************************
 * This computes the collaborators of Linus Torvalds.
 * is the linus example using the adapter.  And slightly revised
 *   algorithm that only ever trades the deltas.
 **************************************************************************/
class Linus : public Application {
public:
    int DEGREES = 4;  // How many degrees of separation form linus?
    int LINUS = 4967;   // The uid of Linus (offset in the user df)
    bool subset = true;

    const char* PROJ = (subset ? "datasets/projects_subset.ltgt" : "datasets/projects.ltgt");
    const char* USER = (subset ? "datasets/users_subset.ltgt" : "datasets/users.ltgt");
    const char* COMM = (subset ? "datasets/commits_subset.ltgt" : "datasets/commits.ltgt");

    DataFrame* projects; //  pid x project name
    DataFrame* users;  // uid x user name
    DataFrame* commits;  // pid x uid x uid
    Set* uSet; // Linus' collaborators
    Set* pSet; // projects of collaborators

    Linus(size_t idx, NetworkIP& net): Application(idx, net) {}

    ~Linus() {
        delete[] PROJ;
        delete[] USER;
        delete[] COMM;
        delete projects;
        delete users;
        delete commits;
        delete uSet;
        delete pSet;
    }

    /** Compute DEGREES of Linus.  */
    void run_() override {
        readInput();
        cout << "READING INPUT" << endl;

        for (size_t i = 0; i < DEGREES; i++) step(i);
    }

    size_t get_file_size(FILE* p_file) // path to file
    {
        fseek(p_file,0,SEEK_END);
        size_t size = ftell(p_file);
        fclose(p_file);
        return size;
    }

    DataFrame* readDataFrameFromFile(const char* filep) {
        cout << "in from file: " << filep << endl;
        FILE* file = fopen(filep, "rb");
        FILE* file_dup = fopen(filep, "rb");
        cout << "fopen file null?: " << (file == nullptr) << endl;
        size_t file_size = get_file_size(file_dup);
        cout << "size of file calced " << file_size << endl;
        cout << "file opened" << endl;


        ///////////////////////////////////////
        SorParser* parser = new SorParser(file, (size_t)0, (size_t)file_size, (size_t)file_size);
        cout << "parser created" << endl;
        parser->guessSchema();
        cout << "schema guessed" << endl;
        try {
            parser->parseFile();
        } catch (const std::exception& e) {
            cout << "PARSE FILE: " << e.what() << endl;
        }

        ///////////////////////////////////////


        delete file;
        cout << "file parsed" << endl;
//        DataFrame* d = new DataFrame(parser->getColumnSet(), parser->_num_columns);
        DataFrame* d = parser->parsed_df;
        cout << "data frame created of SIZE " << d->get_num_rows() << endl;
        delete parser;

        return d;
    }

    /** Node 0 reads three files, cointainng projects, users and commits, and
     *  creates thre dataframes. All other nodes wait and load the three
     *  dataframes. Once we know the size of users and projects, we create
     *  sets of each (uSet and pSet). We also output a data frame with a the
     *  'tagged' users. At this point the dataframe consists of only
     *  Linus. **/
    void readInput() {
        Key* pK = new Key("projs");
        Key* uK = new Key("usrs");
        Key* cK = new Key("comts");
        if (this_node() == 0) {
            // We never put these dfs in the store???
            cout << "Reading..." << endl;
//            commits = DataFrame::fromFile(COMM, cK, kv);
            commits = readDataFrameFromFile(COMM);
            cout << "    " << commits->get_num_rows() << " commits" << endl;
//            projects = DataFrame::fromFile(PROJ, pK, kv);
            projects = readDataFrameFromFile(PROJ);
            cout << "    " << projects->get_num_rows() << " projects" << endl;
//            users = DataFrame::fromFile(USER, uK, kv);
            users = readDataFrameFromFile(USER);
            cout << "    " << users->get_num_rows() << " users" << endl;
            // This dataframe contains the id of Linus.
            //delete
            Key* key = new Key("users-0-0");
            DataFrame::fromScalarInt(key, kv, LINUS);
        } else {
            projects = kv->get(pK);
            users = kv->get(uK);
            commits = kv->get(cK);
        }
        cout << "making sets" <<endl;
        uSet = new Set(users);
        cout << "made a set" << endl;
        pSet = new Set(projects);
        cout << "done making sets" << endl;
    }

    /** Performs a step of the linus calculation. It operates over the three
     *  datafrrames (projects, users, commits), the sets of tagged users and
     *  projects, and the users added in the previous round. */
    void step(int stage) {
        cout << "\n\n\nStage " << stage << endl;
        // Key of the shape: users-stage-0
        //cout << "KV size? " << kv->size << endl;
        //cout << "\n\n\n" << endl;
//        for (size_t m = 0; m < kv->size; m++) {
//            cout << "KV Store Key " << m << ": " << kv->keys[m]->c_str() << endl;
//        }
//        cout << "\n\n\n" << endl;
//        cout << "kv->keys NULL?" << (kv->keys == nullptr) << endl;
//        cout << "kv->keys[0] NULL?" << (kv->keys[0] == nullptr) << endl;
//        cout << "kv->keys[1] NULL?" << (kv->keys[1] == nullptr) << endl;
        StrBuff* s = new StrBuff();
        s->c("users-");
        s->c(stage);
        s->c("-0");
        Key* uK = new Key(s->get());
//        cout << "made key: " << uK->name->c_str() << endl;
        // A df with all the users added on the previous round
        //DataFrame* newUsers = dynamic_cast<DataFrame*>(kv->waitAndGet(uK));
//        if (kv->keys[1] != nullptr) {
//            cout << " keys[1] = " << kv->keys[1]->name->c_str() << endl;
//        }
//        cout << "creating newUsers" << endl;
        DataFrame* newUsers = kv->get(*uK);
//        cout << "GOT new users dataframe from KV key: " << uK->c_str() << endl;
//        cout << "\n\n\n" << endl;
//        for (size_t m = 0; m < kv->size; m++) {
//            cout << "KV Store Key " << m << ": " << kv->keys[m]->c_str() << endl;
//        }
//        cout << "\n\n\n" << endl;


//        cout << "new users null? nrows=" << (newUsers->get_num_rows()) << endl;
        //cout << "made newUsers" << endl;
        Set delta(users);
//        cout << "made delta" << endl;
//        cout << "\n\n\n" << endl;
//        for (size_t m = 0; m < kv->size; m++) {
//            cout << "KV Store Key " << m << ": " << kv->keys[m]->c_str() << endl;
//        }
//        cout << "\n\n\n" << endl;


        SetUpdater* upd = new SetUpdater(delta);
//        cout << "made upd" <<endl;

//        cout << "\n\n\n" << endl;
//        for (size_t m = 0; m < kv->size; m++) {
//            cout << "KV Store Key " << m << ": " << kv->keys[m]->c_str() << endl;
//        }
//        cout << "\n\n\n" << endl;


        newUsers->map(upd); // all of the new users are copied to delta.
        cout << "mapped" << endl;

//        cout << "\n\n\n" << endl;
//        for (size_t m = 0; m < kv->size; m++) {
//            cout << "KV Store Key " << m << ": " << kv->keys[m]->c_str() << endl;
//        }
//        cout << "\n\n\n" << endl;

        //delete newUsers;
        ProjectsTagger* ptagger = new ProjectsTagger(delta, *pSet, projects);
//        cout << "ptagger" << endl;
//
//        cout << "\n\n\n" << endl;
//        for (size_t m = 0; m < kv->size; m++) {
//            cout << "KV Store Key " << m << ": " << kv->keys[m]->c_str() << endl;
//        }
//        cout << "\n\n\n" << endl;

        commits->map(ptagger); // marking all projects touched by delta
        cout << "mapped" <<endl;
//        cout << "\n\n\n" << endl;
//        for (size_t m = 0; m < kv->size; m++) {
//            cout << "KV Store Key " << m << ": " << kv->keys[m]->c_str() << endl;
//        }
//        cout << "\n\n\n" << endl;


        this->kv = merge(ptagger->newProjects, "projects-", stage);
//        cout << "merged" <<endl;

//        cout << "\n\n\n" << endl;
//        for (size_t m = 0; m < kv->size; m++) {
//            cout << "KV Store Key " << m << ": " << &kv->keys[m]->name->cstr_ << endl;
//        }
//        cout << "\n\n\n" << endl;


//        cout << "pset union" <<endl;
        pSet->union_(ptagger->newProjects);

//        cout << "finished pset" << endl;
//        cout << kv->size << endl;
//
//        cout << "\n\n\n" << endl;
//        for (size_t m = 0; m < kv->size; m++) {
//            cout << "KV Store Key " << m << ": " << kv->keys[m]->c_str() << endl;
//        }
//        cout << "\n\n\n" << endl;


//        cout << "utagger" << endl;
        UsersTagger* utagger = new UsersTagger(ptagger->newProjects, *uSet, users);


//        cout << "\n\n\n" << endl;
//        for (size_t m = 0; m < kv->size; m++) {
//            cout << "KV Store Key " << m << ": " << kv->keys[m]->c_str() << endl;
//        }
//        cout << "\n\n\n" << endl;


//        cout << "commits local map" << endl;
        commits->map(utagger);

//        cout << "\n\n\n" << endl;
//        for (size_t m = 0; m < kv->size; m++) {
//            cout << "KV Store Key " << m << ": " << kv->keys[m]->c_str() << endl;
//        }
//        cout << "\n\n\n" << endl;



//        cout << "second merge" << endl;
//
//        cout << "\n\n\n" << endl;
//        for (size_t m = 0; m < kv->size; m++) {
//            cout << "KV Store Key " << m << ": " << kv->keys[m]->c_str() << endl;
//        }
//        cout << "\n\n\n" << endl;
//        cout << "STARTING MERGE" << endl;
        this->kv = merge(utagger->newUsers, "users-", stage + 1);
//        cout << "MERGE FINISHED" << endl;

//        cout << "\n\n\n" << endl;
//        for (size_t m = 0; m < kv->size; m++) {
//            cout << "KV Store Key " << m << ": " << kv->keys[m]->c_str() << endl;
//        }
//        cout << "\n\n\n" << endl;

//        cout << "kv->key[1] null??" << (kv->keys[1] == nullptr) << endl;
//        if (kv->keys[1] != nullptr) {
//            cout << "starting cstr" << endl;
//            kv->keys[2]->c_str();
//            cout << "finished cstr..." << endl;
//            cout << "!!!!kv->keys[1].name null? = " << kv->keys[2]->c_str() << endl;
//        }

//        cout << "uset union" << endl;
        uSet->union_(utagger->newUsers);
//        cout << "    after stage " << stage << ":" << endl;
//        cout << "        tagged projects: " << pSet->size() << endl;
//        cout << "        tagged users: " << uSet->size() << endl;
        //cout << "        kv->keys[1]: " << (kv->keys[1] == nullptr) << "name: " << kv->keys[1]->c_str() << endl;

//        cout << "\n\n\n" << endl;
//        for (size_t m = 0; m < kv->size; m++) {
//            cout << "KV Store Key " << m << ": " << kv->keys[m]->c_str() << endl;
//        }
//        cout << "\n\n\n" << endl;

    }

    /** Gather updates to the given set from all the nodes in the systems.
     * The union of those updates is then published as dataframe.  The key
     * used for the otuput is of the form "name-stage-0" where name is either
     * 'users' or 'projects', stage is the degree of separation being
     * computed.
     */
    KVStore* merge(Set& set, char const* name, int stage) {
//        cout << "in merge" << endl;
        if (this_node() == 0) {
//            cout << "found node 0" << endl;
            for (size_t i = 1; i < arg.num_nodes; ++i) {
                StrBuff* s = new StrBuff();
                s->c(name);
                s->c(stage);
                s->c("-");
                s->c(i);
                Key* nK = new Key(s->get());
                DataFrame* delta = kv->get(*nK);
                cout << "    received delta of " << delta->get_num_rows() << endl;
                cout << " elements from node " << i << endl;
                SetUpdater* upd = new SetUpdater(set);
                delta->map(upd);
                //delete delta;
            }
            cout << "    storing " << set.size() << " merged elements" << endl;
            SetWriter* writer = new SetWriter(set);
//            cout << "making key" << endl;
            StrBuff* h = new StrBuff();
            h->c(name);
            h->c(stage);
            h->c("-0");
            Key* k = new Key(h->get());
//            cout << "k name ------- " << k->name->c_str() << endl;
            //delete DataFrame::fromVisitor(&k, &kv, "I", writer);
//            cout << "calling fromVisitor" << endl;

//            cout << "\n\n\n" << endl;
//            for (size_t m = 0; m < kv->size; m++) {
//                cout << "KV Store Key " << m << ": " << kv->keys[m]->c_str() << endl;
//            }
//            cout << "\n\n\n" << endl;

            DataFrame::fromVisitor(k, kv, "I", writer);
//            cout << "FROM VISITOR FINISHED" << endl;
//
//            cout << "\n\n\n" << endl;
//            for (size_t m = 0; m < kv->size; m++) {
//                cout << "KV Store Key " << m << ": " << &kv->keys[m]->name->cstr_ << endl;
//            }
//            cout << "\n\n\n" << endl;
            return kv;

 //           cout << "kv->keys[1] NULL?" << (kv->keys[1]  == nullptr) << endl;
//            if (kv->keys[1]  != nullptr) {
//                cout << "kv->keys[1] NULL?" << kv->keys[1]->name->c_str() << endl;
//            }
        } else {
            cout << "    sending " << set.size() << " elements to master node" << endl;
            SetWriter* writer = new SetWriter(set);
            Key k(StrBuff(name).c(stage).c("-").c(idx_).get());
//            delete DataFrame::fromVisitor(&k, &kv, "I", writer);
            DataFrame::fromVisitor(k, kv, "I", writer);
            Key mK(StrBuff(name).c(stage).c("-0").get());
            DataFrame* merged = dynamic_cast<DataFrame*>(kv->get(mK));
            cout << "    receiving " << merged->get_num_rows() << " merged elements" << endl;
            SetUpdater* upd = new SetUpdater(set);
            merged->map(upd);
            return nullptr;
            //delete merged;
        }
    }
}; // Linus
