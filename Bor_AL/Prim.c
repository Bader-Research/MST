#include "simple.h"
#include "../grph.h"
#include <sys/types.h>
#include <values.h>

int Prim_dense(int **grph_matrix, int n_vertices,THREADED)
{
   
}

int Prim_sparse(ent_t *grph_list, int n_vertices,THREADED)
{
   int i,j,k,total,min,ind;
   int * D,*live,*min_matrix,*ind_matrix;
 
   D=node_malloc(sizeof(int)*n_vertices,TH);
   live = node_malloc(sizeof(int)*n_vertices,TH);
   min_matrix=node_malloc(sizeof(int)*THREADS,TH);
   ind_matrix=node_malloc(sizeof(int)*THREADS,TH);

   pardo(i,0,n_vertices,1)
   {
	D[i]=MAXINT;
	live[i]=1;
   } 

   node_Barrier();

   on_one_thread {
	live[0]=0;
	D[0]=0;
	for(k=0;k<grph_list[0].n_neighbors;k++)
		D[grph_list[0].my_neighbors[k].w]=grph_list[0].my_neighbors[k].w;
   }
   node_Barrier();

   total=n_vertices-1;
   node_Barrier();

   while(total!=0) {
      if(total%1000==1) printf("total is %d\n",total);
      min = MAXINT;
      ind = 0;
      pardo(i,0,n_vertices,1) 
	if(min>D[i] && live[i]) min=D[i]; 
      min_matrix[MYTHREAD]=min;
      ind_matrix[MYTHREAD]=ind;
 
      node_Barrier();
      on_one_thread{
	for(i=0;i<THREADS;i++)
		if(min>min_matrix[i]){
			min = min_matrix[i];
			ind = ind_matrix[i];	
		}
	live[ind]=0;
      }
      node_Barrier();
      ind = node_Bcast_i(ind,TH);
      min = node_Bcast_i(min,TH);
      total--;

      on_one_thread{
      for(i=0;i<grph_list[ind].n_neighbors;i++)
	D[grph_list[ind].my_neighbors[i].v]=grph_list[ind].my_neighbors[i].w; 
      }
      node_Barrier();
   }

   node_free(D,TH);
   node_free(live,TH);
   node_free(min_matrix,TH);
   node_free(ind_matrix,TH);
}
