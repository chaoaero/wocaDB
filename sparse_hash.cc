/*==================================================================
*   Copyright (C) 2015 All rights reserved.
*   
*   filename:     sparse_hash.cc
*   author:       Meng Weichao
*   created:      2015/12/21
*   description:  
*
================================================================*/
#include "sparse_hash.h"
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include "long_packer.h"
#include "arena.h"
#include "utils.h"
#include "thirdparty/glog/logging.h"
#include<iostream>

SparseHashTable::~SparseHashTable() {
    if(init_ == 1 ) {
        SparseFree(table_, cBuckets);
        if(iter_) {
            HTfree(iter_, sizeof(SparseIterator));
        }

    } else if(init_ == 2) {
        munmap((void *)mmap_metadata_buffer_, mmap_metadata_size_);        
        munmap((void *)mmap_data_buffer_, mmap_data_size_);
    }
}

bool SparseHashTable::InitFromFile(const char *meta_data_path, const char *data_file_path, std::string *errmsg) {

    int meta_fd = open(meta_data_path, O_RDONLY); 
    struct stat sb;
    if(fstat(meta_fd, &sb) == -1)
        return RetFalseWithMsg("open Metadata file failed", errmsg);
    char *buffer = (char *)mmap(NULL, sb.st_size, PROT_READ, MAP_SHARED, meta_fd, 0);
    close(meta_fd);
    if(buffer == MAP_FAILED) {
        return RetFalseWithMsg("map metadata file failed", errmsg);
    }
    char *p = buffer;
    metadata_file_size_ = sb.st_size;

    uint64_t magic_key;
    uint64_t varint_offset = GetVarint64(p, 0, &magic_key);
    if(magic_key != magic_key_) {
        munmap((void *)buffer, sb.st_size);
        return RetFalseWithMsg("check magic key failed", errmsg);
    }

    mmap_metadata_buffer_ = buffer;
    mmap_metadata_size_ = sb.st_size;

    p += varint_offset;
    varint_offset = GetVarint64(p, 0, &cItems);
    
    p += varint_offset;
    varint_offset = GetVarint64(p, 0, &cBuckets);

    cBuckets = SparseAllocate(&table_, cBuckets); 

    cout<<"----------------------------------"<<endl;
    cout<<"cItems is"<<cItems<<endl;
    cout<<"cBuckets is"<<cBuckets<<endl;
    cout<<"magic key is"<<magic_key_<<endl;
    
    cout<<"----------------------------------"<<endl;

    LOG(INFO)<<"-------get the hash meta detail ------------";

    p += varint_offset;

    
    for(uint64_t i = 0; i< SPARSE_GROUPS(cBuckets); i++) {
        for(uint64_t j = 0; j< (1<<LOG_BM_WORDS); j++) {
            varint_offset = GetVarint64(p, 0, &(table_[i].bmOccupied[j]));
            p += varint_offset;
        }
        //cout<<"the bmOccupied is"<<table_[i].bmOccupied[0]<<endl;
        table_[i].cOccupied= SPARSE_POS_TO_OFFSET(table_[i].bmOccupied, 1 << LOG_LOW_BIN_SIZE); 
        //cout<<"the cOccupied is"<<table_[i].cOccupied<<endl;
    }

    for(uint64_t i = 0; i < SPARSE_GROUPS(cBuckets); i++) {
        table_[i].binSparse = (SparseBucket *)p;
        // stride should be cOccupied times sizeof SparseBucket
        p += table_[i].cOccupied * sizeof(SparseBucket);
    }
    

    // get the index buffer
    index_buffer_.append(p);

    //for(uint64_t i = 0; i < SPARSE_GROUPS(cBuckets); i++) {
    //    HTItem *item = table_[i].binSparse;
    //    for(int ww = 0; ww< table_[i].cOccupied; ww++) {
    //        uint64_t keyoffset = item[ww].key;
    //        cout<<"the offset is "<<item[ww].key<<" and the data"<<item[ww].data<<endl;
    //        uint64_t raw_key_size;
    //        uint64_t key_vint_len = GetVarint64(index_buffer_.c_str(),keyoffset, &raw_key_size);
    //        cout<<index_buffer_.substr(keyoffset + key_vint_len, raw_key_size)<<endl;
    //    }
    //}

    // Now mmap the datafile
    int data_fd = open(data_file_path, O_RDONLY);
    struct stat d_sb;
    if(fstat(data_fd, &d_sb) == -1) {
        return RetFalseWithMsg("open datafile failed", errmsg);
    }
    
    mmap_data_buffer_ = (const char *)mmap(NULL, d_sb.st_size, PROT_READ, MAP_SHARED, data_fd, 0);
    close(data_fd);
    if(mmap_data_buffer_ == MAP_FAILED)
        return RetFalseWithMsg("map data file failed", errmsg);
    mmap_data_size_ = d_sb.st_size;
    cout<<"the data file size is"<<mmap_data_size_<<endl;
    init_ = 2;
    return true;

}

uint64_t SparseHashTable::SparseAllocate(SparseBin **pbinSparse, uint64_t cBuckets)
{
    int cGroups = SPARSE_GROUPS(cBuckets);
    *pbinSparse = (SparseBin *) HTscalloc(sizeof(**pbinSparse) * cGroups);
    return cGroups << LOG_LOW_BIN_SIZE;
}

SparseBin* SparseHashTable::SparseFree(SparseBin *binSparse, uint64_t cBuckets)
{
   uint64_t iGroup, cGroups = SPARSE_GROUPS(cBuckets);

   for ( iGroup = 0; iGroup < cGroups; iGroup++ )
      HTfree(binSparse[iGroup].binSparse, (sizeof(*binSparse[iGroup].binSparse)
                       * binSparse[iGroup].cOccupied));
   HTfree(binSparse, sizeof(*binSparse) * cGroups);
   return NULL;
}

int SparseHashTable::SparseIsEmpty(SparseBin *binSparse, uint64_t location)
{
   return !TEST_BITMAP(binSparse[location>>LOG_LOW_BIN_SIZE].bmOccupied,
               MOD2(location, LOG_LOW_BIN_SIZE));
}

SparseBucket* SparseHashTable::SparseFind(SparseBin *binSparse, uint64_t location) {
    if(SparseIsEmpty(binSparse, location))
        return NULL;
    return SPARSE_BUCKET(binSparse, location);
}

string SparseHashTable::Get(const string& key) {
    uint64_t is_empty = 0;
    string result;
    HTItem *item = Find(key, &is_empty);
    if(item == NULL) {
        return result;
    }
    uint64_t data_offset = item->data;
    uint64_t data_len = 0;
    uint64_t value_vint_len = GetVarint64(mmap_data_buffer_ , data_offset, &data_len );

    return string(mmap_data_buffer_ + data_offset + value_vint_len, data_len);
}

HTItem* SparseHashTable::Find(const string& key, uint64_t *is_empty ) {
    uint64_t iBucketFirst;
    HTItem *item;
    uint64_t offset = 0;
    if(table_ == NULL)
        return NULL;
    iBucketFirst = Hash(key.c_str(), key.size(), cBuckets); 
    while(1) {
        item = SparseFind(table_, iBucketFirst); 
        if(item == NULL) {
            if(is_empty )
                *is_empty = iBucketFirst;
        item = SparseFind(table_, iBucketFirst); 
            return NULL;
        } 
        uint64_t key_offset = item->key;
        uint64_t raw_key_size;
        uint64_t key_vint_len = GetVarint64(index_buffer_.c_str(), key_offset, &raw_key_size);
        // the key size is equal to the origin key size
        if(key.size() == raw_key_size) {
            if(index_buffer_.compare(key_offset + key_vint_len, raw_key_size, key ) == 0) { 
                return item;
            }
        }
        offset++;
        iBucketFirst = ((iBucketFirst + (offset * (offset - 1)) / 2) & (cBuckets - 1));
    }

}

SparseBucket* SparseHashTable::SparseInsert(SparseBin *binSparse, SparseBucket *bckInsert, uint64_t location ) {
    SparseBucket *bckPlace;
    uint64_t offset;

    bckPlace = SparseFind(binSparse, location);
    if(bckPlace) {
        *bckPlace = *bckInsert;
        return bckPlace;
    }
    binSparse += (location >> LOG_LOW_BIN_SIZE);
    offset = SPARSE_POS_TO_OFFSET(binSparse->bmOccupied,
            MOD2(location, LOG_LOW_BIN_SIZE));
    binSparse->binSparse = (SparseBucket *) 
        HTsrealloc(binSparse->binSparse,
                sizeof(*binSparse->binSparse) * ++binSparse->cOccupied,
                sizeof(*binSparse->binSparse));
    memmove(binSparse->binSparse + offset+1,
            binSparse->binSparse + offset,
            (binSparse->cOccupied-1 - offset) * sizeof(*binSparse->binSparse));
    binSparse->binSparse[offset] = *bckInsert;
    SET_BITMAP(binSparse->bmOccupied, MOD2(location, LOG_LOW_BIN_SIZE));
    return binSparse->binSparse + offset; 
}

HTItem* SparseHashTable::Insert(const string& key, const string& value) {
    HTItem *item, bckInsert;
    uint64_t is_empty;
    if(table_ == NULL )
        return NULL;
    item = Find(key,&is_empty);
    // key already matches
    if(item) { 
        return item;  
    }
    // write the key and data to our index and data streams
    bckInsert.key = index_buffer_.size();
    bckInsert.data = data_offset_;

    // write key to index string buffer
    PutVarint64(&index_buffer_, key.size());
    index_buffer_.append(key);
    // write value to data file
    uint64_t vint_data_len = PutVarint64ToFile(data_file_, value.size() );
    data_offset_ += vint_data_len;
    data_file_ << value;
    data_offset_ += value.size();
    item = SparseInsert(table_, &bckInsert, is_empty);
    cItems++;
    if(should_rehash()) {
        uint64_t newBuckets = NextPow2((uint64_t)(cItems / load_factor_));
        item = Rehash(newBuckets, item);
    }
    return item;
}

/*************************************************************************\
| SparseFirstBucket()                                                     |
| SparseNextBucket()                                                      |
| SparseCurrentBit()                                                      |
|     Iterate through the occupied buckets of a dense hashtable.  You     |
|     must, of course, have allocated space yourself for the iterator.    |
\*************************************************************************/

SparseBucket* SparseHashTable::SparseNextBucket(SparseIterator *iter)
{
   if ( iter->posOffset != -1 &&      /* not called from FirstBucket()? */
        (++iter->posOffset < iter->binSparse[iter->posGroup].cOccupied) )
      return iter->binSparse[iter->posGroup].binSparse + iter->posOffset;

   iter->posOffset = 0;                         /* start the next group */
   for ( iter->posGroup++;  iter->posGroup < SPARSE_GROUPS(iter->cBuckets);
     iter->posGroup++ )
      if ( iter->binSparse[iter->posGroup].cOccupied > 0 )
     return iter->binSparse[iter->posGroup].binSparse; /* + 0 */
   return NULL;                      /* all remaining groups were empty */
}

SparseBucket* SparseHashTable::SparseFirstBucket(SparseIterator *iter,
                       SparseBin *binSparse, uint64_t cBuckets)
{
   iter->binSparse = binSparse;        /* set it up for NextBucket() */
   iter->cBuckets = cBuckets;
   iter->posOffset = -1;               /* when we advance, we're at 0 */
   iter->posGroup = -1;
   return SparseNextBucket(iter);
}

HTItem* SparseHashTable::Rehash(uint64_t cNewBuckets, HTItem *bckWatch) {
    SparseBin *tableNew;
    uint64_t iBucketFirst;
    HTItem *bck, *bckNew = NULL;
    uint64_t offset;
    cNewBuckets = SparseAllocate(&tableNew, cNewBuckets);

    int count = 0;
    for(bck = SparseFirstBucket(iter_, table_, cBuckets ); ; bck = SparseNextBucket(iter_)) {
        if(bck == NULL) {
            bck = bckWatch;
            if(bck == NULL) {
                break;
            }
        } else if(bck == bckWatch) {
            continue;
        }
        offset = 0;
        uint64_t vint_key_offset = bck->key;
        uint64_t key_len;
        uint64_t vint_kb_size = GetVarint64(index_buffer_.c_str(), vint_key_offset, &key_len); 
        string key = index_buffer_.substr(vint_key_offset + vint_kb_size, key_len );
        
        iBucketFirst = Hash(key.c_str(), key_len, cNewBuckets);
        while(1) {
            if(SparseIsEmpty(tableNew, iBucketFirst))
                break;
            offset++;
            iBucketFirst = (iBucketFirst + offset * (offset - 1) / 2) & (cNewBuckets - 1);
        }

        //for(iBucketFirst = Hash(key.c_str(), key_len, cNewBuckets); !SparseIsEmpty(tableNew, iBucketFirst); iBucketFirst = ((iBucketFirst + (offset * (offset - 1)) / 2) & ( cNewBuckets - 1)))
        // ;
        bckNew = SparseInsert(tableNew, bck, iBucketFirst);
        if(bck == bckWatch)
            break;
    }
    SparseFree(table_, cBuckets); 
    table_ = tableNew;
    set_cbuckets(cNewBuckets);
    return bckNew;
}

bool SparseHashTable::should_rehash() {
    return cItems >= cBuckets * load_factor_;
}

void SparseHashTable::Save() {
    // write the magic number
    //  echo woca.safeline.mobi | sha1sum
    PutVarint64ToFile(metadata_file_, magic_key_);
    PutVarint64ToFile(metadata_file_, cItems);
    PutVarint64ToFile(metadata_file_, cBuckets);


    for(uint64_t i = 0; i < SPARSE_GROUPS(cBuckets); i++) {
        for(uint64_t j = 0; j < (1<< LOG_BM_WORDS); j++) {
            PutVarint64ToFile(metadata_file_, table_[i].bmOccupied[j]);
        }
    }
    
    // write all the binSparse Bucket to metadata_file_
    for(uint64_t i = 0; i< SPARSE_GROUPS(cBuckets) ; i++) {
        metadata_file_.write((const char*)(table_[i].binSparse), table_[i].cOccupied * sizeof(SparseBucket)); 
    }

    // write the index_buffer
    metadata_file_ << index_buffer_;

    // close the metadata_file
    metadata_file_.close();

    data_file_.close();
}

uint64_t SparseHashTable::Hash(const char *key, uint32_t len, uint64_t cBuckets) {
    return HashFunction(key, len) & (cBuckets -1);
}

uint32_t SparseHashTable::HashFunction(const char *key, uint32_t len) {
    //murmurhash3
    uint32_t seed=0x160e0b17;

    static const uint32_t c1 = 0xcc9e2d51;
    static const uint32_t c2 = 0x1b873593;
    static const uint32_t r1 = 15; 
    static const uint32_t r2 = 13; 
    static const uint32_t m = 5;
    static const uint32_t n = 0xe6546b64;

    uint32_t hash = seed;

    const int nblocks = len / 4;
    const uint32_t *blocks = (const uint32_t *) key;
    int i;
    for (i = 0; i < nblocks; i++) {
        uint32_t k = blocks[i];
        k *= c1; 
        k = (k << r1) | (k >> (32 - r1));
        k *= c2; 

        hash ^= k;
        hash = ((hash << r2) | (hash >> (32 - r2))) * m + n;
    }   

    const uint8_t *tail = (const uint8_t *) (key + nblocks * 4); 
    uint32_t k1 = 0;

    switch (len & 3) {
        case 3:
            k1 ^= tail[2] << 16; 
        case 2:
            k1 ^= tail[1] << 8;
        case 1:
            k1 ^= tail[0];

            k1 *= c1; 
            k1 = (k1 << r1) | (k1 >> (32 - r1));
            k1 *= c2; 
            hash ^= k1; 
    }   

    hash ^= len;
    hash ^= (hash >> 16);
    hash *= 0x85ebca6b;
    hash ^= (hash >> 13);
    hash *= 0xc2b2ae35;
    hash ^= (hash >> 16);

    return hash;
}

   /* we need a small function to figure out # of items set in the bm */
// Hamming weight:https://en.wikipedia.org/wiki/Hamming_weight
uint64_t SparseHashTable::EntriesUpto(HTBitmapPart *bm, uint64_t i)
{                                       /* returns # of set bits in 0..i-1 */
   uint64_t retval = 0; 
   static uint64_t rgcBits[256] =             /* # of bits set in one char */
      {0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
       1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
       1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
       2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
       1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
       2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
       2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
       3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
       1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
       2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
       2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
       3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
       2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
       3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
       3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
       4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8};

   if ( i == 0 ) return 0;
   for ( ; i > sizeof(*bm)*8; i -= sizeof(*bm)*8, bm++ )
   {                                       /* think of it as loop unrolling */
#if LOG_WORD_SIZE >= 3                     /* 1 byte per word, or more */
      retval += rgcBits[*bm & 255];        /* get the low byte */
#if LOG_WORD_SIZE >= 4                     /* at least 2 bytes */
      retval += rgcBits[(*bm >> 8) & 255];
#if LOG_WORD_SIZE >= 5                     /* at least 4 bytes */
      retval += rgcBits[(*bm >> 16) & 255];
      retval += rgcBits[(*bm >> 24) & 255];
#if LOG_WORD_SIZE >= 6                     /* 8 bytes! */
      retval += rgcBits[(*bm >> 32) & 255];
      retval += rgcBits[(*bm >> 40) & 255];
      retval += rgcBits[(*bm >> 48) & 255];
      retval += rgcBits[(*bm >> 56) & 255];
#if LOG_WORD_SIZE >= 7                     /* not a concern for a while... */
#error Need to rewrite EntriesUpto to support such big words
#endif   /* >8 bytes */
#endif   /* 8 bytes */
#endif   /* 4 bytes */
#endif   /* 2 bytes */
#endif   /* 1 byte */
   }
   switch ( i ) {                         /* from 0 to 63 */
      case 0:
     return retval;
#if LOG_WORD_SIZE >= 3                     /* 1 byte per word, or more */
      case 1: case 2: case 3: case 4: case 5: case 6: case 7: case 8:
     return (retval + rgcBits[*bm & ((1 << i)-1)]);
#if LOG_WORD_SIZE >= 4                     /* at least 2 bytes */
      case 9: case 10: case 11: case 12: case 13: case 14: case 15: case 16:
     return (retval + rgcBits[*bm & 255] + 
         rgcBits[(*bm >> 8) & ((1 << (i-8))-1)]);
#if LOG_WORD_SIZE >= 5                     /* at least 4 bytes */
      case 17: case 18: case 19: case 20: case 21: case 22: case 23: case 24:
     return (retval + rgcBits[*bm & 255] + rgcBits[(*bm >> 8) & 255] +
         rgcBits[(*bm >> 16) & ((1 << (i-16))-1)]);
      case 25: case 26: case 27: case 28: case 29: case 30: case 31: case 32:
     return (retval + rgcBits[*bm & 255] + rgcBits[(*bm >> 8) & 255] +
         rgcBits[(*bm >> 16) & 255] + 
         rgcBits[(*bm >> 24) & ((1 << (i-24))-1)]);
#if LOG_WORD_SIZE >= 6                     /* 8 bytes! */
      case 33: case 34: case 35: case 36: case 37: case 38: case 39: case 40:
     return (retval + rgcBits[*bm & 255] + rgcBits[(*bm >> 8) & 255] +
         rgcBits[(*bm >> 16) & 255] + rgcBits[(*bm >> 24) & 255] + 
         rgcBits[(*bm >> 32) & ((1 << (i-32))-1)]);
      case 41: case 42: case 43: case 44: case 45: case 46: case 47: case 48:
     return (retval + rgcBits[*bm & 255] + rgcBits[(*bm >> 8) & 255] +
         rgcBits[(*bm >> 16) & 255] + rgcBits[(*bm >> 24) & 255] + 
         rgcBits[(*bm >> 32) & 255] +
         rgcBits[(*bm >> 40) & ((1 << (i-40))-1)]);
      case 49: case 50: case 51: case 52: case 53: case 54: case 55: case 56:
     return (retval + rgcBits[*bm & 255] + rgcBits[(*bm >> 8) & 255] +
         rgcBits[(*bm >> 16) & 255] + rgcBits[(*bm >> 24) & 255] + 
         rgcBits[(*bm >> 32) & 255] + rgcBits[(*bm >> 40) & 255] +
         rgcBits[(*bm >> 48) & ((1 << (i-48))-1)]);
      case 57: case 58: case 59: case 60: case 61: case 62: case 63: case 64:
     return (retval + rgcBits[*bm & 255] + rgcBits[(*bm >> 8) & 255] +
         rgcBits[(*bm >> 16) & 255] + rgcBits[(*bm >> 24) & 255] + 
         rgcBits[(*bm >> 32) & 255] + rgcBits[(*bm >> 40) & 255] +
         rgcBits[(*bm >> 48) & 255] + 
         rgcBits[(*bm >> 56) & ((1 << (i-56))-1)]);
#endif   /* 8 bytes */
#endif   /* 4 bytes */
#endif   /* 2 bytes */
#endif   /* 1 byte */
   }
   assert("" == "word size is too big in EntriesUpto()");
   return -1;
}
