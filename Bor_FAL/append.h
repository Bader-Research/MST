#ifndef _APPEND_H_
#define _APPEND_H_

#include "../grph.h"

#define NCACHE_INTS (1024*64)
#define KEY(A) A.wk_space1
#define ELEMENT(A,index) A[index.list].my_neighbors[index.element]
#define V(A,index) A[index.list].my_neighbors[index.element].v
#define W(A,index) A[index.list].my_neighbors[index.element].w
ent1_t * append_adj_list(ent1_t* grph_list,int *p_n_vertices,int *Label,int n_all_vertices,THREADED);
#endif
