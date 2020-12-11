#include <stdio.h>
#include <stdlib.h>
#include <values.h>
#include "my_heap.h"

heap_t create_heap(int n,int *array_P,comp_func_t *gt, comp_func_t * lt)
{
	int i;
	heap_t heap = malloc(sizeof(struct heap_));
	
	heap->array_P=array_P;
	heap->n_total=n;
	heap->n_elements=0;
	heap->array=malloc(sizeof(HEAP_KEY)*n);	
	heap->gt=gt;
	heap->lt=lt;
	return (heap);
}

int heapify(heap_t heap, int i)
{
	int l,r,smallest;
	HEAP_KEY *A,x,y;
	int * array_P;
	
	array_P=heap->array_P;
	A=heap->array;
	l=LEFT(i);
	r=RIGHT(i);
	
	if(l<heap->n_elements && heap->lt(A[l],A[i]))
		smallest=l;
	else smallest=i;
	
	if(r<heap->n_elements && heap->lt(A[r],A[smallest]))
		smallest=r;
		
	if(smallest!=i) {
		x=A[i];
		y=A[smallest];
		A[i]=A[smallest];
		A[smallest]=x;
		if(array_P) {
			array_P[ID(y)]=i;
			array_P[ID(x)]=smallest;
		}
		heapify(heap,smallest);
		
	}
		
}

int heap_extract_min(heap_t heap, HEAP_KEY * key)
{
	HEAP_KEY * A,min;
	
	if(heap->n_elements<1) {
		printf("heap underflow\n");
		return (-1);
	}
	
	A = heap->array;
	min = A[0];
	A[0]=A[heap->n_elements-1];
	heap->n_elements--;
	heap->array_P[ID(min)]=-1;
	heapify(heap,0);
	*key=min;
}

int heap_insert(heap_t heap, HEAP_KEY key)
{

	HEAP_KEY * A;
	int i;
	int * array_P;
	
	array_P=heap->array_P;
	heap->n_elements++;
	if(heap->n_elements>heap->n_total) {
		printf("HEAP overflow\n");
		return (-1);
	}
	i=heap->n_elements;
	i--;
	
	A=heap->array;
	while(i>0 && heap->gt(A[PARENT(i)],key))
	{
		A[i]=A[PARENT(i)];
		array_P[ID(A[PARENT(i)])]=i;
		i=PARENT(i);
	}
	A[i]=key;
	array_P[ID(key)]=i;
	return (i);
}

int heap_decrease_key(heap_t heap, int position, HEAP_KEY new_key)
{
	HEAP_KEY * A;
	int i,*array_P;
	
	array_P=heap->array_P;
	A=heap->array;
	if(!heap->gt(A[position],new_key)) return(position);	
	
	i=position;
	while(i>0 &&  heap->gt(A[PARENT(i)],new_key))
	{
		A[i]=A[PARENT(i)];	
		array_P[ID(A[PARENT(i)])]=i;
		i=PARENT(i);	
	}
	A[i]=new_key;
	array_P[ID(new_key)]=i;
	return(i);
}

int delete_heap(heap_t heap)
{
	free(heap->array);
	free(heap);
}

int heap_is_empty(heap_t heap)
{
  if(heap->n_elements==0) return (1);
  else return(0);
}

int heap_clean(heap_t heap)
{
        heap->n_elements=0;
}
