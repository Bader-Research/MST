#include "simple.h"
#include "../grph.h"
#include "append.h"
#include <sys/types.h>
#include <values.h>
#include "my_malloc.h"

int Boruvka_sparse_nocompact(ent1_t *grph_list, int n_vertices,THREADED)
{

  hrtime_t start,end,s1,t1;
  double interval,findmin_t=0,append_t=0,housekeep_t=0;

  int newly_dead=0,*p_alive,*p_total,*D,*Label,*Min;
  int min,min_ind,k,l,w,i,j,total=0;
  int n_all_vertices;
  adj_t * pTmp;
  
  n_all_vertices = n_vertices;
  D = node_malloc(sizeof(int)*n_vertices,TH);
  Label = node_malloc(sizeof(int)*n_vertices,TH);
 
  Min = node_malloc(sizeof(int)*n_vertices,TH);
  
  pardo(i,0,n_all_vertices,1) Label[i]=i;
  node_Barrier();
	
  while(1) { 
  
  	start = gethrtime();

	pardo(i,0,n_vertices,1){
	 D[i]=i;
	}
	node_Barrier();

	s1 = gethrtime();

	pardo(i,0,n_vertices,1)
	{
		min=MAXINT;
		min_ind=-1;
		pTmp = grph_list[i].head;
		while(pTmp!=NULL){
			for(k=0;k<pTmp->n_neighbors;k++)
          	{
            		j=pTmp->my_neighbors[k].v;
            		if(min>pTmp->my_neighbors[k].w && i!=Label[j]){
                 		min = pTmp->my_neighbors[k].w;
                 		min_ind=j;
					}
            }
			pTmp=pTmp->next;
		}
		if(min_ind!=-1) D[i]=Label[min_ind];
		if(min_ind!=-1) Min[i]=min;
    }

	node_Barrier();
	t1 = gethrtime();
	interval = t1 - s1;
	interval = interval /NANO;
	on_one printf(" Time used for find-min is %f s\n",interval);

	findmin_t += interval;

	s1 = gethrtime();
#if 1	
	pardo(i,0,n_vertices,1)
	{
          if(D[D[i]]!=i|| (D[D[i]]==i && i<D[i]) ){
                 total+=Min[i];
          }
     }
	node_Barrier();
#endif

	pardo(i,0,n_vertices,1)
		if(i==D[D[i]] && i<D[i]) D[i]=i;
	node_Barrier();

	pardo(i,0,n_vertices,1)
		while(D[i]!=D[D[i]]){
		 D[D[i]]=D[D[D[i]]];
		 D[i]=D[D[i]];
		} 
	node_Barrier();
	
	pardo(i,0,n_all_vertices,1)
		Label[i]=D[Label[i]];	
	
	node_Barrier();

	pardo(i,0,n_vertices,1)
		grph_list[i].wk_space1=D[i];	
	node_Barrier();
	
	t1 = gethrtime();
	interval = t1 - s1;
	interval = interval /NANO;
	on_one printf("Time used for housekeeping is %f s\n",interval);
	housekeep_t+=interval;	

#if 1
	s1 = gethrtime();
#endif
	grph_list=append_adj_list(grph_list,&n_vertices,Label,n_all_vertices,TH);
	if(n_vertices<=1) break;

#if 1
	t1 = gethrtime();
	interval=t1-s1;
	interval=interval/NANO;
	on_one printf("Time used for appending  is %f s\n",interval);
	append_t += interval;
#endif
	node_Barrier();
	end = gethrtime();
	interval=end-start;
	on_one_thread printf("Time for this round: %f s\n\n", interval/NANO);

  } /*while*/

  total=node_Reduce_i(total,SUM,TH);
  on_one printf("total weight is %d \n",total);
  on_one printf("METRICS: time used for find_min, housekeeping, append is %f, %f %f \n",findmin_t,housekeep_t,append_t);
  on_one printf("==================================================\n\n");

  node_Barrier();
  node_free(D,TH);
  return(total);
}
