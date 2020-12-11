#include <stdio.h>
#include "simple.h"
#include "../grph.h"


int _Sort(edge_t ** edge_list, int n_edges, int *D, int n_vertices,THREADED)
{
	int i,j;
	edge_t * p;
	
	on_one_thread{
		for(i=0;i<n_edges-1;i++)
			for(j=i+1;j<n_edges;j++){
				if(D[edge_list[i]->v1] > D[edge_list[j]->v1] || (D[edge_list[i]->v1] == D[edge_list[j]->v1] && \
				D[edge_list[i]->v2] > D[edge_list[j]->v2]))
				{
					p = edge_list[i];
					edge_list[i]=edge_list[j];
					edge_list[j]=p;
				}
				
			}
	}
}
