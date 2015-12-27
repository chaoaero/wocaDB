/*==================================================================
*   Copyright (C) 2015 All rights reserved.
*   
*   filename:     arena.h
*   author:       Meng Weichao
*   created:      2015/12/23
*   description:  
*
================================================================*/
#ifndef __ARENA_H__
#define __ARENA_H__

void* HTsmalloc(uint64_t size)
{
    void *retval;
    if ( size == 0 )
        return NULL;
    retval = (void *)malloc(size);
    if ( !retval )
    {
        fprintf(stderr, "HTsmalloc: Unable to allocate %lu bytes of memory\n", size);
        exit(1);
    }
    return retval;
}

void *HTscalloc(uint64_t size)
{
   void *retval;

   retval = (void *)calloc(size, 1);
   if ( !retval && size > 0 )
   {
      fprintf(stderr, "HTscalloc: Unable to allocate %lu bytes of memory\n",
          size);
      exit(1);
   }
   return retval;
}

/* HTsrealloc() -- safe calloc
 *    grows the amount of memory from a source, or crashes if
 *    the allocation fails.
 */
static void *HTsrealloc(void *ptr, uint64_t new_size, long delta)
{
   if ( ptr == NULL )
      return HTsmalloc(new_size);
   ptr = realloc(ptr, new_size);
   if ( !ptr && new_size > 0 )
   {
      fprintf(stderr, "HTsrealloc: Unable to reallocate %lu bytes of memory\n",
          new_size);
      exit(1);
   }
   return ptr;
}

/* HTfree() -- keep track of memory use
 *    frees memory using free, but updates count of how much memory
 *    is being used.
 */
void HTfree(void *ptr, uint64_t size)
{
   if ( size > 0 )         /* some systems seem to not like freeing NULL */
      free(ptr);
}

#endif //__ARENA_H__
