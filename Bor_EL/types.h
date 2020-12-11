
#include "../grph.h"

/*
#define MAX_VAL 2147483647 
#define MAX_VAL_m1 (MAX_VAL-1)
*/

#define ERROR 0
#define LOCATION 0
#define SORTTYPE 1 

typedef  edge_t* TYPE;

typedef struct record
  {TYPE val;
   int start;
  } record_t;

typedef struct split
  {TYPE val;
   int count;
  } split_t;

typedef struct {
    int thread_id;
    TYPE *buffer1;
    TYPE *buffer2;
    split_t *splitters;
    int size;
    int M;
    int p;
    int s;
    int *counts;
    TYPE *sample1;
    int bits;
    int value_range;
    int threads;
    } sl_params;

