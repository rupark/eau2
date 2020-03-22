# eau2 System Architecture Document #
## Introduction ##
In this document we will describe the architecture and implementation of the eau2 system.
Thus far, the eau2 system is able to read in SoR files and construct DataFrames, our system's internal representation
of columnar SoR files. The system is also contains a Key Value Store which is a hash-map like structure which can associate Keys with DataFrames.
In the following sections we will discuss these features in more technical detail as well as
describe the future work involved. 
## Architecture ##
_In this section, we describe the various part of eau2 at a high-level._

TODO
## Implementation ##
In this section, we describe how the system is built. Currently the functionality for each milestone is available, but not connected.
### Team 4500NE's Sorer
In this project, we utilized [Team 4500NE's sorer](https://github.ccs.neu.edu/euhlmann/CS4500-A1-part1). This sorer had the benefit
of already being written in CwC which ours was not. Team 4500NE had their own definition of Column, which can be found in 
column_prov.h. In our application, we use their sorer to parse a given SoR file into their definition of Column. We then translate
those columns into our definition of DataFrame. By executing the program in this way, we preserve the sanctity of Team 4500NE's
code, and do not have to retest their already tested functionality.  
### KVStore
The KVStore class found in kvstore.h represents a Key Value Store. KVStore contains an array of Key* and DataFrame*, as well as a size. 
The KVStore contains three methods: get, put, and getAndWait. get takes in a Key and returns the DataFrame associated with that Key in the store.
put adds a given Key and DataFrame to the KVStore arrays and increases the size. The getAndWait method is yet to be finished. Ultimately, the KVStore is very similar to a hashmap.
### DataFrame
The most important file in our program is dataframe.h. A DataFrame
consists of a list of Columns. There are four types of Columns: IntColumn, 
BoolColumn, FloatColumn, and StringColumn. Each Column contains an array of values of its kind.
Primarily in our program we use the push_back method to add elements to Columns. DataFrame's also contain a Schema,
which describes the columns in a DataFrame. For example, a DataFrame consisting of an IntColumn, BoolColumn, FloatColumn, and StringColumn in that order
would have a Schema with a types String of "IBFS". 

To interact with the sorer, we created a special DataFrame constructor which creates an instance of a DataFrame using Team 4500NE's sorer. In this system,
we assume that DataFrame's will not need to be modified once constructed. 

Another important method on DataFrame is the fromArray method. This method constructs a DataFrame from a given array of doubles and 
adds it to the given KVStore under the given Key. It then returns the constructed DataFrame.
## Use cases ##
```
//Creating a new KVStore
KVStore kv = *new KVStore();

//Initializing some variables
size_t SZ = 1000*1000;
double* vals = new double[SZ];
double sum = 0;
for (size_t i = 0; i < SZ; ++i) sum += vals[i] = i;

//Creating a new Key
Key key("triv",0);

//Creating a DataFrame consisting of the doubles in vals using the static fromArray method.
//The fromArray method also associates df with key in the KVStore
DataFrame* df = DataFrame::fromArray(&key, &kv, SZ, vals);
//Checking to see if the DataFrame was set with the correct values from vals
assert(df->get_double(0,1) == 1);

//Retrieving the DataFrame associated with key from the KVStore
DataFrame* df2 = kv.get(key);
for (size_t i = 0; i < SZ; ++i) sum -= df2->get_double(0,i);
//The values in df2 are the same as df
assert(sum==0);
```
## Open questions ##
* What is the objective of getAndWait (what are we waiting for)?
* Why is delete not working in Milestone 2?
* What is the purpose of the Architecture section?
## Status ##
| Milestone Number | Status  |
|:---:|:---:|
| 1 | ✓ |
| 2 | ✓ |
| 3 | ✗ |
| 4 | ✗ |
| 5 | ✗ |
