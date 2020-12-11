#ifndef _GRPH_H_
#define _GRPH_H_

#define DENSE 1 
#define SPARSE 0
#define PROFILING 0 
#define NANO 1


#if PROFILING
#define MAX_P 20
double compact_list_time[MAX_P];
double sort_time[MAX_P];
double compact_time[MAX_P];
double distinct_time[MAX_P];
#endif

typedef struct ele {
int v,w;
} ele_t;

typedef struct ent {
 int n_neighbors;
 ele_t * my_neighbors;
 int wk_space1;
} ent_t;

typedef ent_t WV;

typedef struct adj_l {
  int n_neighbors;
  ele_t * my_neighbors;
  struct adj_l * next; 
} adj_t;

typedef struct ent_1 {
 int n_neighbors;
 adj_t * head,*tail;
 int wk_space1;
} ent1_t;

typedef struct edge {
  int v1, v2;
  int w;
} edge_t;

int read_graph(char * graph_name,int * n_vertices, int *d_or_s,ent_t** p_grph_list, int *** p_grph_matrix );
WV* r_graph(int n,int m);
#endif
