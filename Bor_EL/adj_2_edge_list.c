#include "simple.h"
#include "../grph.h"


int adj_2_edge_list(ent_t *grph_list, int n_vertices, edge_t ***p_edge_list, int*p_n_edges,int * p_buf_size,int M,THREADED)
{
	int *degree_arr,i,j,n_edges,start_p,n_buf_size;
	edge_t* edge_list;
	
	degree_arr=node_malloc(sizeof(int)*n_vertices, TH);
	
	pardo(i,0,n_vertices,1)
		degree_arr[i]=grph_list[i].n_neighbors;

	node_Barrier();
	
	prefix_sum(degree_arr,n_vertices,TH);
	
	node_Barrier();
	
	n_edges=degree_arr[n_vertices-1];
	/* helman's sort code needs a buffer larger than the number of edges */
	n_buf_size = (n_edges + 2*(((int) (n_edges/M)) + THREADS*THREADS))*sizeof(edge_t *);	
	edge_list=node_malloc(sizeof(edge_t)*n_edges,TH);
	(*p_edge_list)=(edge_t **)node_malloc(sizeof(edge_t *)*n_buf_size,TH);
	
	pardo(i,0,n_vertices,1)
	{
		if(i==0) start_p =0;
		else start_p=degree_arr[i-1];
		
		for(j=0;j<grph_list[i].n_neighbors;j++)
		{
			edge_list[start_p+j].v1=i;
			edge_list[start_p+j].v2=grph_list[i].my_neighbors[j].v;
			edge_list[start_p+j].w=grph_list[i].my_neighbors[j].w;
		}
	}
	
	node_Barrier();
	
	pardo(i,0,n_edges,1)
	{
		(*p_edge_list)[i]=&(edge_list[i]);
	}
	node_free(degree_arr,TH);
	*p_n_edges=n_edges;
	*p_buf_size = n_buf_size;
}
