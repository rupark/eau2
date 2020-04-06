#pragma once

#include "../key.h"
#include "application.h"
#include "../dataframe.h"
#include "../kvstore.h"


/**
 * Goal for M3: instance of Application class
 */
class Demo : public Application {
public:
    Key main = *new Key("main", 0);
    Key verify = *new Key("verif", 0);
    Key check = *new Key("ck", 0);

    Demo(size_t idx) : Application(idx) {}

    void run_() override {
        switch (this_node()) {
            case 0:
                producer();
                break;
            case 1:
                counter();
                break;
            case 2:
                summarizer();
        }
    }

    void producer() {
        size_t SZ = 100 * 1000;
        double *vals = new double[SZ];
        double sum = 0;
        for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;
        DataFrame::fromArray(&main, &kv, SZ, vals);
        DataFrame::fromScalar(&check, &kv, sum);
    }

    void counter() {
        DataFrame *v = kv.waitAndGet(main);
        size_t sum = 0;
        for (size_t i = 0; i < 100 * 1000; ++i) sum += v->get_double(0, i);
        p("The sum is  ").pln(sum);
        DataFrame::fromScalar(&verify, &kv, sum);
    }

    void summarizer() {
        DataFrame *result = kv.waitAndGet(verify);
        DataFrame *expected = kv.waitAndGet(check);
        pln(expected->get_double(0, 0) == result->get_double(0, 0) ? "SUCCESS" : "FAILURE");
    }
};
