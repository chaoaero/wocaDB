/*==================================================================
*   Copyright (C) 2015 All rights reserved.
*   
*   filename:     sparse_hash.h
*   author:       Meng Weichao
*   created:      2015/12/21
*   description:  
*
================================================================*/
//http://www.leexh.com/blog/2014/10/25/popcount-problem/
#ifndef __SPARSE_HASH_H__
#define __SPARSE_HASH_H__

#include <sys/types.h> 
#include <assert.h>
#include<stdint.h>
#include<stdio.h>
#include<iostream>
//#include "arena.h"
#include<fstream>
using namespace std;

#define LOG_WORD_SIZE          5   /* log_2(sizeof(uint64_t)) [in bits] */

/* The following gives a speed/time tradeoff: how many buckets are  *
 * in each bin.  0 gives 32 buckets/bin, which is a good number.    */
#ifndef LOG_BM_WORDS
#define LOG_BM_WORDS        0      /* each group has 2^L_B_W * 32 buckets */
#endif

#define LOG_LOW_BIN_SIZE    ( LOG_BM_WORDS+LOG_WORD_SIZE )
#define SPARSE_GROUPS(cBuckets) ( (((cBuckets)-1) >> LOG_LOW_BIN_SIZE) + 1 )

#ifndef MIN_HASH_SIZE
#define MIN_HASH_SIZE       512    /* ht size when first created */
//#define MIN_HASH_SIZE       1024 /* ht size when first created */
#endif

typedef struct {
    uint64_t data;     
    uint64_t key;       
} HTItem;

#ifndef SparseBucket            /* by default, each bucket holds an HTItem */
#define SparseBucket            HTItem
#endif

/* the following are useful for bitmaps */
/* Format is like this (if 1 word = 4 bits):  3210 7654 ba98 fedc ... */
typedef uint64_t          HTBitmapPart;  /* this has to be unsigned, for >> */
typedef HTBitmapPart   HTBitmap[1<<LOG_BM_WORDS];
//typedef uint64_t          HTOffset; /* something big enough to hold offsets */

#define MOD2(i, logmod)      ( (i) & ((1<<(logmod))-1) )
#define DIV_NUM_ENTRIES(i)   ( (i) >> LOG_WORD_SIZE )
#define MOD_NUM_ENTRIES(i)   ( MOD2(i, LOG_WORD_SIZE) )
#define MODBIT(i)            ( ((uint64_t)1) << MOD_NUM_ENTRIES(i) )

#define TEST_BITMAP(bm, i)   ( (bm)[DIV_NUM_ENTRIES(i)] & MODBIT(i) ? 1 : 0 )
#define SET_BITMAP(bm, i)    (bm)[DIV_NUM_ENTRIES(i)] |= MODBIT(i)
#define CLEAR_BITMAP(bm, i)  (bm)[DIV_NUM_ENTRIES(i)] &= ~MODBIT(i)

typedef struct SparseBin {
    SparseBucket *binSparse;
    HTBitmap bmOccupied;
    short cOccupied;
} Table;

typedef struct SparseIterator {
   uint64_t posGroup;
   uint64_t  posOffset;
   SparseBin *binSparse;     /* state info, to avoid args for NextBucket() */
   uint64_t cBuckets;
} SparseIterator;


#define SPARSE_POS_TO_OFFSET(bm, i)   ( EntriesUpto(&((bm)[0]), i) )
#define SPARSE_BUCKET(bin, location)  \
   ( (bin)[(location) >> LOG_LOW_BIN_SIZE].binSparse +                     \
      SPARSE_POS_TO_OFFSET((bin)[(location)>>LOG_LOW_BIN_SIZE].bmOccupied, \
                   MOD2(location, LOG_LOW_BIN_SIZE)) )

class SparseHashTable {

public:
    SparseHashTable(string metadata_name = "ht_temp_metadata.dat", string data_name = "ht_temp_val.dat"): cItems(0),
        data_offset_(0), load_factor_(0.5), magic_key_(0x64158d3183e1a6f2ull) {

        data_file_.open(data_name.c_str(), ofstream::out | ofstream::binary);
        metadata_file_.open(metadata_name.c_str(),  ios::out | ios::binary);
        cBuckets = SparseAllocate(&table_, MIN_HASH_SIZE);
        iter_ = (SparseIterator *)malloc(sizeof(SparseIterator));
        init_ = 1;
    }

    ~SparseHashTable();

    uint64_t SparseAllocate(SparseBin **pbinSparse, uint64_t cBuckets);
    SparseBin* SparseFree(SparseBin *binSparse, uint64_t cBuckets);
    int SparseIsEmpty(SparseBin *binSparse, uint64_t location);
    SparseBucket* SparseFind(SparseBin *binSparse, uint64_t location); 
    HTItem* Find(const string& key, uint64_t *is_empty ); 
    SparseBucket *SparseInsert(SparseBin *binSparse, SparseBucket *bckInsert, uint64_t location ); 
    void set_cbuckets(uint64_t c_bucket_) {
        cBuckets = c_bucket_;
    }

    // This is the entrance to insert data in hash
    HTItem* Insert(const string& key, const string& value); 
    // This is the function we use after building the data.
    string Get(const string& key);


    SparseBucket* SparseFirstBucket(SparseIterator *iter,SparseBin *binSparse, uint64_t cBuckets);
    SparseBucket* SparseNextBucket(SparseIterator *iter);
    bool InitFromFile(const char *meta_data_path, const char *data_file_path, std::string *errmsg = NULL);

    HTItem* Rehash(uint64_t cNewBuckets, HTItem *bckWatch); 
    void Save(); 
    bool should_rehash();
    uint64_t Hash(const char *key, uint32_t len, uint64_t cBuckets);

public:
    static uint32_t HashFunction(const char *key, uint32_t len);
    static uint64_t EntriesUpto(HTBitmapPart *bm, uint64_t i);

private:
        
    inline bool RetFalseWithMsg(const char *in_errmsg, std::string *out_errmsg)
    {
        if (out_errmsg != NULL && in_errmsg != NULL)
            *out_errmsg = in_errmsg;
        return false;
    }

private:
    uint64_t cItems;        /* number of items currently in the hashtable */
    uint64_t cBuckets;      /* size of the table */
    Table *table_;        /* The actual contents of the hashtable */
    HTItem *posLastFind; /* position of last Find() command */
    SparseIterator *iter_; /* used in First/NextBucket */
    string index_buffer_;
    ofstream data_file_;
    ofstream metadata_file_;
    uint64_t data_offset_;
    float load_factor_;
    uint64_t magic_key_;
    uint32_t init_; // 0 未初始化; 1 动态分别内存; 2 从文件读入
    uint64_t metadata_file_size_;
    uint64_t data_file_size_;
    const char *mmap_data_buffer_;
    const char *mmap_metadata_buffer_;
    uint64_t mmap_metadata_size_;
    uint64_t mmap_data_size_;
};


#endif //__SPARSE_HASH_H__
