#ifndef _MY_MALLOC_H_
#define _MY_MALLOC_H_

#include <stdio.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/mman.h>
#include "simple.h"

int init_mem(size_t size,THREADED);
void * my_malloc(size_t size,THREADED);
void clear_mem(THREADED);
#endif

