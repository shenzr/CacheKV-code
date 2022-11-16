# CacheKV
## 1 Introduction
This is the implementation of CacheKV described in the paper "**Redesigning High-Performance LSM-based Key-Value Stores with Persistent CPU Caches**". CacheKV is a LSM-Tree based KV store utilizing Persistent CPU Cache provided by Intel Optane Persistent Memory 200 Series.
We implement CacheKV based on [NoveLSM] (https://github.com/sudarsunkannan/lsm_nvm).

## 2 Compilation and Run CacheKV
### 2.1 Tools
CacheKV relies on Intel(R) RDT Software Package (https://github.com/intel/intel-cmt-cat).
To run CacheKV, please install it first (https://github.com/intel/intel-cmt-cat/blob/master/INSTALL).

### 2.2 Compilation
The GCC version in our environment is 7.5.0.
```
  $ cd hoard
  $ ./compile_install_hoard.sh
  $ cd ..
  $ make -j8
```

### 2.3 Run
First set the environment variables and then run the DB_Bench benchmark.
```
  $ source scripts/setvars.sh
  $ scripts/run_cachekv_dbbench.sh
```

