#include "simple.h"
#include "../grph.h"
#include "merge.h"
#include "values.h"
#include "my_malloc.h"
#include "prefix_sum.h"

#define DEBUG 1

/* (1) Merge the the graph list. (this list is first sorted according to
 the D(i) value. So that nodes with the same D(i) value are consequtive.
 Merge such that any element with the same 
D(i) value (or the same wk_space value) should be merged into a
single copy. Also Within the single list, there should not be 
repeated values After merging, compact the whole thing into one new adj-list.
*/
ele_t* mergesort_nr_2(ele_t *Buffer1,ele_t*Buffer2,int col_size);

ent_t * compact_adj_list_arr (ent_t * grph_list,int *p_n_vertices,ele_t *tmp_arr,ele_t* sort_buff,THREADED)
{
  int my_local_entries=0,my_start,my_end,n,i,j,k,new_len,n_vertices,new_n_vertices,d,l,key;
  int * buff;
  index_t_ left,right,q;
  ele_t * new_ele_arr,*result_buff; 
  ent_t * tmp_list;

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
  tmp_list = node_malloc(sizeof(ent_t)*n_vertices,TH);
  all_radixsort_smp_s2(n_vertices,grph_list,tmp_list,TH);
  node_free(grph_list,TH);
  grph_list = tmp_list;
  node_Barrier();

  pardo(i,1,n_vertices,1)
  	if(grph_list[i].wk_space1<grph_list[i-1].wk_space1) printf("Wrong\n");

  node_Barrier();
  
#if 1
  t1 = gethrtime();
  interval2=(t1-s1);
  interval2=interval2/NANO;
  on_one_thread printf("====Radix sorting time is %f s\n", interval2);
#endif

#if PROFILING
  s1 = gethrtime();
#endif
      
  partition_list(grph_list, n_vertices, &my_start, &my_end,TH);

#if 0
  printf("THREAD %d:<my_start,my_end>=<%d,%d>, %d elements\n",MYTHREAD,my_start,my_end,my_end-my_start);  
#endif
 
  buff = node_malloc(sizeof(int)*n_vertices,TH);
  node_Barrier();
  pardo(i,0,n_vertices,1) buff[i]=0; 
  node_Barrier();
  
  pardo(i,0,n_vertices,1){
  	buff[grph_list[i].wk_space1]=1;
  } 
  node_Barrier();    
  prefix_sum(buff,n_vertices,TH);
  node_Barrier();

#if PROFILING
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
  
  tmp_list=node_malloc(sizeof(ent_t)*new_n_vertices,TH);  
  
  
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

#if PROFILING		
		nsorted+=n;
#endif		
		/*now I have k consequtive nodes starting from i to be merged*/
		/*The number of adj elements for these nodes is n*/
		left.list=i;
		left.element=0;
		right.list=i+k-1;
  		right.element=grph_list[right.list].n_neighbors-1;	

#if PROFILING
		s1=gethrtime();
#endif		

		copy_out(grph_list,left,right,tmp_arr);	
#if 0
		Qsort_arr(tmp_arr,0,n-1);
			
		if(n<50) insertsort(tmp_arr,n);
		else mergesort_nr(tmp_arr,n);
		
#endif

		if(n<50) insertsort(tmp_arr,n);
		else {
			result_buff=mergesort_nr_2(tmp_arr,sort_buff,n);
			if(result_buff==sort_buff)
			{	
				sort_buff = tmp_arr;
				tmp_arr  = result_buff;
			}
		}	

#if PROFILING	
  		t1=gethrtime();
  		interval=t1-s1;
  		interval=interval/NANO;  
  		sort_time[MYTHREAD]+=interval;
#endif  


#if 0
		check_sort_arr(tmp_arr,0,n-1);
#endif		
		new_len=0;

#if PROFILING
		s1=gethrtime();
#endif
		
		for(l=0;l<n;l++)
		{
			if(tmp_arr[l].v==key) continue;
			if(l==0) new_len++;
			else if(tmp_arr[l].v!=tmp_arr[l-1].v) new_len++;
		}	
#if 0		
		printf("new_len==%d, n is %d\n",new_len,n);	
#endif
		new_ele_arr = my_malloc(sizeof(ele_t)*new_len,TH);
		
		k=0;
		for(l=0;l<n;l++)
		{
			if(tmp_arr[l].v==key) continue;
			if(l==0) {
				new_ele_arr[k].v=buff[tmp_arr[l].v]-1;
				new_ele_arr[k++].w=tmp_arr[l].w;
			}
			else if(tmp_arr[l].v!=tmp_arr[l-1].v) {
					new_ele_arr[k].v=buff[tmp_arr[l].v]-1;
					new_ele_arr[k++].w=tmp_arr[l].w;
				}
		}

#if PROFILING
		t1=gethrtime();
		interval1+=(t1-s1);
#endif
			
		d=grph_list[i].wk_space1;
		d=buff[d]-1;
		
		tmp_list[d].my_neighbors=new_ele_arr;
		tmp_list[d].n_neighbors=new_len;
		i=j;
#if 0
		free(tmp_arr);
#endif

	}

#if 0 
	t2 = gethrtime();
	interval4=t2-s2;
	interval4=interval4/NANO;
	printf("====THREAD %d: compact_list time used for my turn is %f s\n", MYTHREAD,interval4);
#endif
		
  node_Barrier();

#if 0  
  pardo(i,0,n_vertices,1)
  	free(grph_list[i].my_neighbors);
  node_Barrier();
#endif

#if PROFILING
  end=gethrtime();
  interval=end-start;
  interval=interval/NANO;  
  compact_list_time[MYTHREAD]+=interval;
#endif
  

#if PROFILING
	printf("===== THREAD %d: my sort time for this round is %f s\n",MYTHREAD,sort_time[MYTHREAD]);
	printf("===== THREAD %d: my number of sorted elements is %d \n",MYTHREAD,nsorted);
	printf("===== THREAD %d: the time for rearranging data is %f s\n",MYTHREAD, interval1/NANO);	
#endif  
  *p_n_vertices=new_n_vertices;
  node_free(grph_list,TH);
  node_free(buff,TH);
  return(tmp_list);	  
}


#if 0

/* instead of copying the list into a new array and sort, just set up a new array of pointers and sort the new array
of pointers. More read redirection, yet less writes */

ent_t * compact_adj_list_arr_ptr (ent_t * grph_list,int *p_n_vertices,ele_t **tmp_arr,THREADED)
{
  int my_local_entries=0,my_start,my_end,n,i,j,k,new_len,n_vertices,new_n_vertices,d,l,key;
  int * buff;
  index_t_ left,right,q;
  ele_t * new_ele_arr; 
  ent_t * tmp_list;

#if DEBUG 
  int nsorted=0;
  hrtime_t start,end,s1,t1,s2,t2;
  double interval=0,interval1=0,interval2=0,interval3=0,interval4;
  start=gethrtime();
#endif
 

#if PROFILING
  s1 = gethrtime();
#endif
        
  n_vertices=*p_n_vertices;
  tmp_list = node_malloc(sizeof(ent_t)*n_vertices,TH);
  all_radixsort_smp_s2(n_vertices,grph_list,tmp_list,TH);
  node_free(grph_list,TH);
  grph_list = tmp_list;
  node_Barrier();

#if PROFILING
  t1 = gethrtime();
  interval2=(t1-s1);
  interval2=interval2/NANO;
  on_one_thread printf("====Radix sorting time is %f s\n", interval2);
#endif
  
   
#if 0  
  /*every thread has a range of consequtive nodes to work on*/
  if(MYTHREAD==0) my_start=0;
  else {
  		my_start=(n_vertices/THREADS)*MYTHREAD;
		while(my_start<n_vertices && KEY(grph_list[my_start])==KEY(grph_list[my_start-1])) my_start++;
  }
  
  if(MYTHREAD==THREADS-1) my_end=n_vertices-1;
  else {
  		my_end=(n_vertices/THREADS)*(MYTHREAD+1)-1;
		while(my_end<n_vertices-1 && KEY(grph_list[my_end])==KEY(grph_list[my_end+1])) my_end++;
  }

  printf("THREAD %d:<my_start,my_end>=<%d,%d>\n",MYTHREAD,my_start,my_end);
#endif

#if PROFILING
  s1 = gethrtime();
#endif
      
  node_Barrier();
  partition_list(grph_list, n_vertices, &my_start, &my_end,TH);

  printf("THREAD %d:<my_start,my_end>=<%d,%d>, %d elements\n",MYTHREAD,my_start,my_end,my_end-my_start);  
  
  buff = node_malloc(sizeof(int)*n_vertices,TH);
  node_Barrier();
  pardo(i,0,n_vertices,1) buff[i]=0; 
  node_Barrier();
  
  pardo(i,0,n_vertices,1)
  	buff[grph_list[i].wk_space1]=1; 
  node_Barrier();    
  prefix_sum(buff,n_vertices,TH);
  node_Barrier();

#if PROFILING
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
  
  tmp_list=node_malloc(sizeof(ent_t)*new_n_vertices,TH);  
  
  
#if PROFILING  
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

#if PROFILING		
		nsorted+=n;
#endif		
		/*now I have k consequtive nodes starting from i to be merged*/
		/*The number of adj elements for these nodes is n*/
		left.list=i;
		left.element=0;
		right.list=i+k-1;
  		right.element=grph_list[right.list].n_neighbors-1;	

#if PROFILING
		s1=gethrtime();
		printf("%d elements to be sorted\n", n);
#endif		
#if 0
		tmp_arr = (ele_t*)malloc(sizeof(ele_t)*(n*2));	
#endif

		copy_out_ptr(grph_list,left,right,tmp_arr);	
#if 0		
		Qsort_arr_ptr(tmp_arr,0,n-1);
#endif
		if(n<50) insertsort(tmp_arr,n);
		else mergesort_nr(tmp_arr,n);
			
		
		
#if PROFILING	
  		t1=gethrtime();
  		interval=t1-s1;
  		interval=interval/NANO;  
  		sort_time[MYTHREAD]+=interval;
#endif  


#if 0
		check_sort_arr(tmp_arr,0,n-1);
#endif		
		new_len=0;

#if PROFILING
		s1=gethrtime();
#endif
		
		for(l=0;l<n;l++)
		{
			if(tmp_arr[l].v==key) continue;
			if(l==0) new_len++;
			else if(tmp_arr[l].v!=tmp_arr[l-1].v) new_len++;
		}	
		if(new_len==n) printf("new_len==n\n");	
		new_ele_arr = malloc(sizeof(ele_t)*new_len);
		
		k=0;
		for(l=0;l<n;l++)
		{
			if(tmp_arr[l].v==key) continue;
			if(l==0) {
				new_ele_arr[k].v=buff[tmp_arr[l].v]-1;
				new_ele_arr[k++].w=tmp_arr[l].w;
			}
			else if(tmp_arr[l].v!=tmp_arr[l-1].v) {
					new_ele_arr[k].v=buff[tmp_arr[l].v]-1;
					new_ele_arr[k++].w=tmp_arr[l].w;
				}
		}

#if PROFILING
		t1=gethrtime();
		interval1+=(t1-s1);
#endif
			
		d=grph_list[i].wk_space1;
		d=buff[d]-1;
		
		tmp_list[d].my_neighbors=new_ele_arr;
		tmp_list[d].n_neighbors=new_len;
		i=j;
#if 0
		free(tmp_arr);
#endif

	}

#if PROFILING
	t2 = gethrtime();
	interval4=t2-s2;
	interval4=interval4/NANO;
	printf("====THREAD %d: time used for my turn is %f s\n", MYTHREAD,interval4);
#endif
		
  node_Barrier();

#if 0  
  pardo(i,0,n_vertices,1)
  	free(grph_list[i].my_neighbors);
  node_Barrier();
#endif

#if PROFILING
  end=gethrtime();
  interval=end-start;
  interval=interval/NANO;  
  compact_list_time[MYTHREAD]+=interval;
#endif
  

#if PROFILING
	printf("===== THREAD %d: my sort time for this round is %f s\n",MYTHREAD,sort_time[MYTHREAD]);
	printf("===== THREAD %d: my number of sorted elements is %d \n",MYTHREAD,nsorted);
	printf("===== THREAD %d: the time for rearranging data is %f s\n",MYTHREAD, interval1/NANO);	
#endif  
  *p_n_vertices=new_n_vertices;
  node_free(grph_list,TH);
  node_free(buff,TH);
  return(tmp_list);	  
}

#endif

int copy_out(ent_t * grph_list, index_t_ left, index_t_ right,ele_t * tmp_arr)
{
	int l=0;
	index_t_ i;
	
	for(i=left; leq_(i,right);incr_(grph_list,&i))
	{
		tmp_arr[l++]=ELEMENT(grph_list,i);
	} 
}

/*When sorting, the elements in the list should be in their D(i) values*/
int Qsort_arr(ele_t * tmp_arr,int left,int right)
{

   int q;
  
   if(left<right){
   	q=Partition_arr(tmp_arr,left,right);
	Qsort_arr(tmp_arr,left,q);	
	Qsort_arr(tmp_arr,q+1,right);
	
   }
    
}

int Partition_arr(ele_t *tmp_arr,int p, int r)
{
	ele_t x,y;
	int i,j;
	int v,w;
	
	x=tmp_arr[p];
	i=p-1;	
	j=r+1;
	
	while(1)
	{
		do{
			j--;
			v=tmp_arr[j].v;
			w=tmp_arr[j].w;			
		}while(j>=p && (v>x.v || (v==x.v && w>x.w)));
		
		do{
			i++;
			v=tmp_arr[i].v;
			w=tmp_arr[i].w;
		}while(i<=r && (v<x.v || (v==x.v && w<x.w)));
		
		if(i<j){
#if 0		
			printf("%d,%d<---->%d,%d\n",i.list,i.element,j.list,j.element);
#endif			
			y=tmp_arr[i];
			tmp_arr[i]=tmp_arr[j];
			tmp_arr[j]=y;
		} else return (j);
	}
}


int check_sort_arr(ele_t * tmp_arr, int left, int right)
{
	int wrong = 0;
	int i,j;
	
	i=left;
	while(i<right)
	{	
		j=i+1;
		
		if(tmp_arr[i].v> tmp_arr[j].v || (tmp_arr[i].v==tmp_arr[j].v && tmp_arr[i].w>tmp_arr[j].w)) 
		{
			wrong =1;
			printf("sorting wrong, n is %d\n",right);
			printf(" i is %d,i->v is %d, j->v is %d\n",i,tmp_arr[i].v,tmp_arr[j].v);
			break;			
		}

		i=j;
	}
#if 0
	if(wrong) {
		for(i=left;i<=right;i++)
			printf(" (%d,%d) ", tmp_arr[i].v, tmp_arr[i].w);
	}
	printf("\n");
#endif
}
