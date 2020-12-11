#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <sys/time.h>
#include <strings.h>
#include <values.h>
#include "../grph.h"

typedef ele_t DATA_TYPE;

DATA_TYPE MAX_VAL={MAXINT,MAXINT},MAX_VAL_m1={MAXINT-1,MAXINT-1};

void insertsort(DATA_TYPE *A, int n) {

  register DATA_TYPE item;
  register int i,j;

  for (i=1 ; i<n ; i++) {
    item = A[i];
    j = i-1;
    while ((j>=0)&&(item.v < A[j].v || (item.v==A[j].v && item.w<A[j].w))) {
      A[j+1] = A[j];
      j--;
    }
    A[j+1] = item;
  }
}

DATA_TYPE * mergesort_nr_2(DATA_TYPE *Buffer1,
					 DATA_TYPE *Buffer2,
					 int col_size) {


  int
    i, j, k,
    values, times,
    adjustments, last1, last2,
    tot, tot1, tot2,
    rem;

  DATA_TYPE
    *start_ptr,
    *finish_ptr,
    *temp_ptr,
    *ex_ptr,
    *s1_ptr, *s2_ptr;

  start_ptr = Buffer1;
  finish_ptr = Buffer2;

  times = (int) ceil(log((double) col_size)/log((double) 2)); 

  adjustments = 0;
  values = col_size;

  if (col_size & 1)
    temp_ptr = start_ptr + col_size - 1;
  else  
    temp_ptr = start_ptr + col_size;    
  while (start_ptr < temp_ptr) {
    if ((*start_ptr).v > (*(start_ptr+1)).v || ((*start_ptr).v == (*(start_ptr+1)).v) && (*start_ptr).w > (*(start_ptr+1)).w) {
      *(finish_ptr++) = *(start_ptr+1);
      *(finish_ptr) = *(start_ptr);
      if ((*(finish_ptr++)).v > MAX_VAL_m1.v) {
	adjustments++;
	(*(finish_ptr - 1)) = MAX_VAL_m1;
      }
      *(finish_ptr++) = MAX_VAL;
      start_ptr+=2;
    }
    else {
      *(finish_ptr++) = *(start_ptr++);
      *(finish_ptr) = *(start_ptr++);
      if ((*(finish_ptr++)).v > MAX_VAL_m1.v) {
	adjustments++;
	(*(finish_ptr-1)) = MAX_VAL_m1;
	if ((*(finish_ptr - 2)).v > MAX_VAL_m1.v) {
	  adjustments++;
	  (*(finish_ptr-2)) = MAX_VAL_m1; 
	}
      }

      *(finish_ptr++) = MAX_VAL;
    }
  } 

#if 0 
	if(col_size==52){
	printf("1:--------------------------------\n");
	for(i=0;i<col_size;i++)
		printf(" (%d,%d) ", Buffer2[i].v,Buffer2[i].w);
	printf("\n");
 	}	
#endif

  last1 = last2 = 2;

  if (col_size & 1) {
    *(finish_ptr) = *(temp_ptr);
    if ((*(finish_ptr++)).v > MAX_VAL_m1.v) {
      adjustments++;
      (*(finish_ptr-1)) = MAX_VAL_m1;
    }
    *(finish_ptr) = MAX_VAL;
    last2 = 1;
  }

  ex_ptr = Buffer1;
  Buffer1 = Buffer2;
  Buffer2 = ex_ptr;

  tot = 2;
  tot1 = 1;
  tot2 = col_size >> 1;

  for (i=2;i<times;i++) {
    start_ptr = Buffer1;
    finish_ptr = Buffer2;
    tot <<= 1; 
    tot1 <<= 1;
    tot2 >>= 1;
    for (j=1;j<tot2;j++) {
      s2_ptr = (s1_ptr = (start_ptr)) + tot1 + 1;  
      temp_ptr = finish_ptr + tot;
      while (finish_ptr < temp_ptr) {
	if ((*s1_ptr).v < (*s2_ptr).v || (((*s1_ptr).v == (*s2_ptr).v )&& ((*s1_ptr).w < (*s2_ptr).w)))
	  *(finish_ptr++) = *(s1_ptr++);
	else 
	  *(finish_ptr++) = *(s2_ptr++);
      }
      *(finish_ptr++) = MAX_VAL;       
      start_ptr += (tot + 2);
    }
  
    rem = (values & (tot - 1));
  
    if (! rem) {
      s2_ptr = (s1_ptr = (start_ptr)) + tot1 + 1;  
      temp_ptr = finish_ptr + tot;
      while (finish_ptr < temp_ptr) {
	if ((*s1_ptr).v < (*s2_ptr).v || (((*s1_ptr).v == (*s2_ptr).v) && ((*s1_ptr).w < (*s2_ptr).w)))
	  *(finish_ptr++) = *(s1_ptr++);
	else 
	  *(finish_ptr++) = *(s2_ptr++);
      }
      *(finish_ptr++) = MAX_VAL;       
      last1 = last2 = tot;
    }
    else {
      if (rem <= tot1) {
	s1_ptr = start_ptr;
	for (k=0;k<=tot1;k++)
	  *(finish_ptr + k) = *(s1_ptr + k);
	finish_ptr += (tot1 + 1); 
	start_ptr += (tot1 + 1);
	s2_ptr = (s1_ptr = (start_ptr)) + last1 + 1;  
	temp_ptr = finish_ptr + last1 + last2;
	while (finish_ptr < temp_ptr) {
	  if (((*s1_ptr).v < (*s2_ptr).v) || (((*s1_ptr).v == (*s2_ptr).v) &&( (*s1_ptr).w < (*s2_ptr).w)))
	    *(finish_ptr++) = *(s1_ptr++);
	  else 
	    *(finish_ptr++) = *(s2_ptr++);
	}
	*(finish_ptr++) = MAX_VAL;       
	last2 += last1;
	last1 = tot1;
      }
      else {
	s2_ptr = (s1_ptr = (start_ptr)) + tot1 + 1;  
	temp_ptr = finish_ptr + tot;
	while (finish_ptr < temp_ptr) {
	  if (((*s1_ptr).v < (*s2_ptr).v) || (((*s1_ptr).v == (*s2_ptr).v )&&( (*s1_ptr).w < (*s2_ptr).w)))
	    *(finish_ptr++) = *(s1_ptr++);
	  else 
	    *(finish_ptr++) = *(s2_ptr++);
	} 
	*(finish_ptr++) = MAX_VAL;       
	start_ptr += (tot + 2);
	s2_ptr = (s1_ptr = (start_ptr)) + last1 + 1;  
	temp_ptr = finish_ptr + last1 + last2;
	while (finish_ptr < temp_ptr) {
	  if (((*s1_ptr).v < (*s2_ptr).v) || (((*s1_ptr).v == (*s2_ptr).v) && ((*s1_ptr).w < (*s2_ptr).w)))
	    *(finish_ptr++) = *(s1_ptr++);
	  else 
	    *(finish_ptr++) = *(s2_ptr++);
	}
	*(finish_ptr++) = MAX_VAL;       
	last2 += last1;
	last1 = tot;
      }
    }
    ex_ptr = Buffer1;
    Buffer1 = Buffer2;
    Buffer2 = ex_ptr;
  } 

#if 0 
	if(col_size==52){
	printf("2:--------------------------------\n");
	for(i=0;i<col_size;i++)
		printf(" (%d,%d) ", Buffer2[i].v,Buffer2[i].w);
	printf("\n");
 	}	
#endif
  start_ptr = Buffer1;
  finish_ptr = Buffer2;
  s2_ptr = (s1_ptr = (start_ptr)) + last1 + 1;  
  temp_ptr = finish_ptr + last1 + last2;

  while (finish_ptr < temp_ptr) {
    if ((*s1_ptr).v < (*s2_ptr).v || ((*s1_ptr).v == (*s2_ptr).v && (*s1_ptr).w < (*s2_ptr).w))
      *(finish_ptr++) = *(s1_ptr++);
    else 
      *(finish_ptr++) = *(s2_ptr++);
  }

  finish_ptr = Buffer2 + values;;
  temp_ptr = finish_ptr - adjustments;

  while ((--finish_ptr) >= temp_ptr) 
    *finish_ptr =  MAX_VAL;

  finish_ptr = Buffer2;

  return(finish_ptr);

}

void mergesort_nr(DATA_TYPE *A, int elems) {
  DATA_TYPE *finlist, *B;

  B = (DATA_TYPE *)malloc(2*elems*sizeof(DATA_TYPE));
  assert_malloc(B);
  
  finlist = mergesort_nr_2(A, B, elems);
  if (finlist == B)
    bcopy(B,A,elems*sizeof(DATA_TYPE));

  free(B);
  
}
