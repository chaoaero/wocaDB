/*==================================================================
*   Copyright (C) 2015 All rights reserved.
*   
*   filename:     utils.h
*   author:       Meng Weichao
*   created:      2015/12/23
*   description:  
*
================================================================*/
#ifndef __UTILS_H__
#define __UTILS_H__

uint64_t NextPow2(uint64_t x)    /* returns next power of 2 > x, or 2^31 */
{
   if ( ((x << 1) >> 1) != x )    /* next power of 2 overflows */
      x >>= 1;                    /* so we return highest power of 2 we can */
   while ( (x & (x-1)) != 0 )     /* blacks out all but the top bit */
      x &= (x-1);
   return x << 1;                 /* makes it the *next* power of 2 */
}

#endif //__UTILS_H__
