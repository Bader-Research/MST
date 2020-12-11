#include "simple.h"
#include "../grph.h"
#include "append.h"
#include "values.h"
#include "my_malloc.h"

#define DEBUG 1

/* append all the adj list that are adjacent to the same supervertex */

ent1_t * append_adj_list(ent1_t* grph_list,int *p_n_vertices,int *Label,int n_all_vertices,THREADED)
{
  int my_local_entries=0,my_start,my_end,n,i,j,k,new_len,n_vertices,new_n_vertices,d,l,key,max;
  int * buff;
  adj_t * pTmp;
  ent1_t * tmp_list;

#if DEBUG 
  int nsorted=0;
  hrtime_t start,end,s1,t1,s2,t2;
  double interval=0,interval1=0,interval2=0,interval3=0,interval4;
  start=gethrtime();
#endif
 

#if 1
  s1 = gethrtime();
#endif
 
  n_vertices=*p_n_vertices;
  tmp_list = node_malloc(sizeof(ent1_t)*n_vertices,TH);

#if 0
  max = 0;
  pardo(i,0,n_vertices,1)
  	if(max<Label[i]) max = Label[i];
  max = node_Reduce_i(max,MAX,TH);
  on_one printf(" max is %d \n", max);
#endif
 
  all_radixsort_smp_s2(n_vertices,grph_list,tmp_list,TH);
  node_free(grph_list,TH);
  grph_list = tmp_list;
  node_Barrier();

#if 1
  t1 = gethrtime();
  interval2=(t1-s1);
  interval2=interval2/NANO;
  on_one_thread printf("====Radix sorting time is %f s\n", interval2);
#endif

#if 1 
  s1 = gethrtime();
#endif
      
  partition_list(grph_list, n_vertices, &my_start, &my_end,TH);

#if 0
  printf("THREAD %d:<my_start,my_end>=<%d,%d>, %d elements\n",MYTHREAD,my_start,my_end,my_end-my_start);  
#endif
 
  buff = node_malloc(sizeof(int)*n_vertices,TH);
  pardo(i,0,n_vertices,1) buff[i]=0; 
  node_Barrier();
  
  pardo(i,0,n_vertices,1) buff[grph_list[i].wk_space1]=1; 
  node_Barrier();    
  prefix_sum(buff,n_vertices,TH);
  node_Barrier();
  
  pardo(i,0,n_all_vertices,1)
  	Label[i]=buff[Label[i]]-1;

#if 1
  t1 = gethrtime();
  interval3=(t1-s1);
  interval3=interval3/NANO;
  on_one_thread printf("====partition list and prefix sum time %f s\n",interval3);
#endif  
  
  new_n_vertices=buff[n_vertices-1];
  on_one_thread printf("The number of new vertices is %d\n",new_n_vertices);
  if(new_n_vertices <=1) {
  	*p_n_vertices=new_n_vertices;
 	 node_free(grph_list,TH);
  	node_free(buff,TH);
  	return (NULL);	 
  }
  
  tmp_list=node_malloc(sizeof(ent1_t)*new_n_vertices,TH);  
  
  
#if 1
  s2= gethrtime();
#endif
  
  i=my_start;
  
  while(i<=my_end)
  	{
		k=1;
		j=i+1;
		n=grph_list[i].n_neighbors;
		key=KEY(grph_list[i]);
		while(KEY(grph_list[j])==KEY(grph_list[i]) && j<n_vertices){
		 n+=grph_list[j].n_neighbors;
		 j++;
		 k++;
		}
			
		d=grph_list[i].wk_space1;
		d=buff[d]-1;
		
		tmp_list[d].n_neighbors=n;
		tmp_list[d].head = grph_list[i].head;
		pTmp = grph_list[i].tail;
		for(l=i+1;l<j;l++){
			pTmp->next = grph_list[l].head;
			pTmp = grph_list[l].tail;
		}
		tmp_list[d].tail=pTmp;
		i=j;
	}

#if 1
	t2 = gethrtime();
	interval4=t2-s2;
	interval4=interval4/NANO;
#if 0
	printf("====THREAD %d: compact_list time used for my turn is %f s\n", MYTHREAD,interval4);
#endif
#endif
		
  node_Barrier();

  *p_n_vertices=new_n_vertices;
  node_free(grph_list,TH);
  node_free(buff,TH);
  return(tmp_list);	  
}

int partition_list(ent1_t * grph_list, int n_vertices, int * mystart, int *myend,THREADED)
{

	int * buff,i,j,seg,start,end,search_start,*arr, n_elements;
	
	if(n_vertices<THREADS) {
		if(MYTHREAD==0) {
			*mystart=0;
			*myend=n_vertices-1;
		} else {
		*mystart=0;
		*myend=-1;
		}	
		return(0);
	}
	
	
	buff = node_malloc(sizeof(int)*n_vertices,TH);
	arr = node_malloc(sizeof(int)*THREADS,TH);
	pardo(i,0,n_vertices,1)
	{
		buff[i]=grph_list[i].n_neighbors;
	}
	
	node_Barrier();
	prefix_sum(buff,n_vertices,TH);
	
	n_elements=buff[n_vertices-1];
	seg = n_elements/THREADS;
	on_one_thread printf("n_elements is %d\n", n_elements);
	
	pardo(i,0,n_vertices,1)
	{
		search_start=i;
		break;
	}
	
	j=search_start;
	while(buff[j]<(MYTHREAD+1)*seg && j<n_vertices) j++;
	while(buff[j-1]>=(MYTHREAD+1)*seg && j>=0) j--;
	
	if (MYTHREAD==THREADS-1) * myend=n_vertices-1;
	else 
		*myend=j;	
	
	arr[MYTHREAD]=j;
	node_Barrier();
	
	if(MYTHREAD==0) *mystart=0;
	else *mystart=arr[MYTHREAD-1]+1;

	 while(*mystart>0 && grph_list[*mystart].wk_space1==grph_list[*mystart-1].wk_space1)
		(*mystart)++;
	while((*myend<n_vertices-1) && grph_list[*myend].wk_space1==grph_list[*myend+1].wk_space1)
		(*myend)++;
	
	if(*mystart<0) *mystart=0;
	
	node_Barrier();
	node_free(buff,TH);
	node_free(arr,TH);
}
