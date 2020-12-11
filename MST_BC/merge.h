#ifndef _MERGE_H_
#define _MERGE_H_

#include "../grph.h"


#define NCACHE_INTS (1024*64)
#define KEY(A) A.wk_space1
#define ELEMENT(A,index) A[index.list].my_neighbors[index.element]
#define V(A,index) A[index.list].my_neighbors[index.element].v
#define W(A,index) A[index.list].my_neighbors[index.element].w

typedef struct index_{
    int list,element;
} index_t_;

index_t_ Partition(ent_t * grph_list,index_t_ q, index_t_ r);
void compact(ele_t * new_ele_arr,ent_t * grph_list, index_t_ start, index_t_
end,int *buff,int d);
void incr_(ent_t * grph_list, index_t_ *ind);
void decr_(ent_t * grph_list, index_t_ *ind);  
ent_t * compact_adj_list (ent_t * grph_list,int *p_n_vertices,THREADED);
ent_t * compact_adj_list_2 (ent_t * grph_list,int *p_n_vertices,int *buff1,THREADED);
ent_t * compact_adj_list_3 (ent_t * grph_list,int *p_n_vertices,THREADED);
ent_t * compact_adj_list_arr (ent_t * grph_list,int *p_n_vertices,ele_t *tmp_arr,ele_t *sort_buff,THREADED);
#endif
