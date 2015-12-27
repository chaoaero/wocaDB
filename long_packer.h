/*==================================================================
*   Copyright (C) 2015 All rights reserved.
*   
*   filename:     coding.h
*   author:       Meng Weichao
*   created:      2015/12/23
*   description:  
*
================================================================*/
#ifndef __CODING_H__
#define __CODING_H__
#include<stdint.h>
// http://www.leexh.com/blog/2014/10/25/popcount-problem/

// Endian-neutral encoding:
// * Fixed-length numbers are encoded with least-significant byte first
// * In addition we support variable length "varint" encoding
// * Strings are encoded prefixed by their length in varint format
//

// Get the exact key length of the key.
// * res   : raw key length
// * index : index offset in the string
// * input : input string
// * count : the return value for targetting the real string destination
uint64_t GetVarint64(const char *input, uint64_t index, uint64_t *res) {
    uint64_t result = 0;
    uint64_t count = 0;
    for(uint64_t shift = 0; shift <= 63; shift += 7) {
        uint64_t byte = input[index++];
        count++;
        if(byte & 128) {
            result |= ((byte & 127) << shift);
        } else {
            result |= (byte << shift);
            *res = result;
            return count; 
        }
    }
    return count;
}


char* EncodeVarint64(char* dst, uint64_t v) {
  static const uint64_t B = 128;
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  while (v >= B) {
    *(ptr++) = (v & (B-1)) | B;
    v >>= 7;
  }
  *(ptr++) = static_cast<unsigned char>(v);
  return reinterpret_cast<char*>(ptr);
}

// Encode uint64 code to variable byte code
// * dst    : target string to append
// * v      : integer to be encoded
void PutVarint64(string* dst, uint64_t v) {
    char buf[10];
    char* ptr = EncodeVarint64(buf, v);
    uint64_t vint_len = ptr - buf;
    dst->append(buf, vint_len);
}

uint64_t PutVarint64ToFile(ofstream& fout, uint64_t v) {
    char buf[10];
    char* ptr = EncodeVarint64(buf, v);
    uint64_t vint_len = ptr - buf;
    fout.write(buf, vint_len);
    return vint_len;
}

#endif //__CODING_H__
