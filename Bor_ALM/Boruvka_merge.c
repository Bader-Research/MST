#include "simple.h"
#include "../grph.h"
#include "merge.h"
#include <sys/types.h>
#include <values.h>
#include "my_malloc.h"

int Boruvka_sparse_merge(ent_t *grph_list, int n_vertices,THREADED)
{

  hrtime_t start,end,s1,t1,s_findmin,t_findmin, s_compact,t_compact,s_cc,t_cc;
  double interval1=0,interval2=0,interval3=0,interval;
  double ti_findmin=0,ti_compact=0,ti_cc=0;
  ele_t * tmp_arr,*sort_buff;
  int * buff1;

  int newly_dead=0,*p_alive,*p_total,*D,*dead_matrix,*total_matrix;
  int min,min_ind,k,l,w,i,j,total=0;

  
  D = node_malloc(sizeof(int)*n_vertices,TH);
  dead_matrix = node_malloc(sizeof(int)*THREADS,TH);
  total_matrix = node_malloc(sizeof(int)*THREADS,TH);
  p_alive = node_malloc(sizeof(int),TH);
  p_total = node_malloc(sizeof(int),TH);

  tmp_arr = my_malloc(sizeof(ele_t)*(4*n_vertices),TH);
  sort_buff =my_malloc(sizeof(ele_t)*(4*n_vertices),TH);
  buff1 = my_malloc(sizeof(int)*n_vertices,TH);

  on_one_thread {
	*p_alive = n_vertices;
	*p_total = 0;
		
  }
  
  node_Barrier();
  	
  while(1) { 
  
  	start = gethrtime();
	on_one_thread printf(" Alive vertices:%d, total weights:%d\n",n_vertices,*p_total); 
	newly_dead=0;
	total=0;

	pardo(i,0,n_vertices,1){
	 D[i]=i;
    }
	node_Barrier();

	s_findmin = gethrtime();
	pardo(i,0,n_vertices,1)
	{
		min=MAXINT;
		min_ind=-1;
		for(k=0;k<grph_list[i].n_neighbors;k++)
          	{
            		j=grph_list[i].my_neighbors[k].v;
            		if(i!=j && min>grph_list[i].my_neighbors[k].w ){
                 		min = grph_list[i].my_neighbors[k].w;
                 		min_ind=j;
					}
            }
		if(min_ind!=-1) D[i]=min_ind;
    }

	node_Barrier();
	t_findmin = gethrtime();
	interval = t_findmin-s_findmin;
	interval = interval/NANO;
	ti_findmin+=interval;
	
#if 0	
	pardo(i,0,n_vertices,1)
		printf("D[%d]=%d  ",i,D[i]);	
		
	pardo(i,0,n_vertices,1)
	{
          if(D[D[i]]!=i|| (D[D[i]]==i && i<D[i]) ){
                 total+=Min[i];
                 printf("<%d,%d>\n",i,D[i]);
                 newly_dead++;
          }
     }
	 
	dead_matrix[MYTHREAD]=newly_dead;
	total_matrix[MYTHREAD]=total;
	node_Barrier();

	on_one_thread {
		for(i=0;i<THREADS;i++){
			*p_alive -= dead_matrix[i];
			*p_total += total_matrix[i];
		}
	}
	node_Barrier();
#endif

	s_cc = gethrtime();
	pardo(i,0,n_vertices,1)
		if(i==D[D[i]] && i<D[i]) D[i]=i;
	node_Barrier();

	pardo(i,0,n_vertices,1)
		while(D[i]!=D[D[i]]){
			D[D[i]]=D[D[D[i]]];
			D[i]=D[D[i]]; 
        }    
	node_Barrier();
	
	pardo(i,0,n_vertices,1)
	{
		grph_list[i].wk_space1=D[i];
		for(j=0;j<grph_list[i].n_neighbors;j++)
			grph_list[i].my_neighbors[j].v=D[grph_list[i].my_neighbors[j].v];
	}
	
	node_Barrier();
	t_cc = gethrtime();
	interval = t_cc - s_cc;
	interval = interval/NANO;
	ti_cc += interval;

	s_compact = gethrtime();
	grph_list=compact_adj_list_arr(grph_list,&n_vertices,tmp_arr,sort_buff,TH);
	t_compact = gethrtime();
	interval = t_compact-s_compact;
	interval = interval/NANO;
	ti_compact+=interval;
	
	if(n_vertices<=1) break;
		
	on_one_thread printf("**** compact_adj_list_arr: n_vertices is now %d\n",n_vertices);
	node_Barrier();
	
	end = gethrtime();
	interval1=end-start;
	on_one_thread printf("=== Time for this round: %f s\n\n", interval1/NANO);

  } /*while*/

  on_one printf("The total weight is %d\n",*p_total);
  on_one printf("METRICS: Time used for finding_min is %f s, conn-comp is %f s, compact is %f s\n", ti_findmin, ti_cc, ti_compact);
  node_Barrier();
  //free(tmp_arr);
  node_free(p_alive,TH); 
  node_free(p_total,TH); 
  node_free(dead_matrix,TH);
  node_free(total_matrix,TH);
  node_free(D,TH);
}

