#ifndef _MY_HEAP_H_
#define _MY_HEAP_H_

/* This heap use consequtive storage "arrary" to build the tree. 
   Because it is more efficient than using dynamically allocated
   structures. One complication is that you need to know in 
   advance how many elements are there.
*/

#include "my_type.h"

#define PARENT(i)	((i+1)/2-1)
#define LEFT(i)   	(2*(i+1)-1)
#define RIGHT(i)  	(2*(i+1))

typedef int comp_func_t (KEY, KEY); 

struct heap_ {
int n_total;     /* total elements in the storage */
int n_elements;  /* number of elements currently in the heap*/
KEY *array; /* the storage for the heap structure*/
int *array_P; /* the position array */
comp_func_t * gt,*lt;
};

typedef struct heap_ * heap_t;

heap_t create_heap(int n, int * array_P,comp_func_t *gt, comp_func_t * lt);
int heapify(heap_t heap, int i); 
int clean_heap(heap_t heap);
int heap_extract_min(heap_t pheap, KEY * key);
int heap_insert(heap_t pheap, KEY key);
int heap_is_empty(heap_t heap);
#endif /*_MY_HEAP_H_*/
