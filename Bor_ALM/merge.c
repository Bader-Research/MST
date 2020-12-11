#include "simple.h"
#include "../grph.h"
#include "merge.h"
#include "values.h"

#define DEBUG 1


/* (1) Merge the the graph list. (this list is first sorted according to
 the D(i) value. So that nodes with the same D(i) value are consequtive.
 Merge such that any element with the same 
D(i) value (or the same wk_space value) should be merged into a
single copy. Also Within the single list, there should not be 
repeated values 
  After merging, compact the whole thing into one new adj-list.
*/

ent_t * compact_adj_list (ent_t * grph_list,int *p_n_vertices,THREADED)
{
  int my_local_entries=0,my_start,my_end,n,i,j,k,new_len,n_vertices,new_n_vertices,d,l,key;
  int * buff;
  index_t_ left,right,q;
  ele_t * new_ele_arr; 
  ent_t * tmp_list;

#if DEBUG 
  hrtime_t start,end,s1,t1;
  double interval;
  start=gethrtime();
#endif
  
  
  
  n_vertices=*p_n_vertices;
  tmp_list = node_malloc(sizeof(ent_t)*n_vertices,TH);
  start = gethrtime();
  all_radixsort_smp_s2(n_vertices,grph_list,tmp_list,TH);
  end = gethrtime();
  interval = end - start;
  on_one printf("time used to radix sort is %f s\n", interval /1000000000); 
  node_free(grph_list,TH);
  grph_list = tmp_list;
  node_Barrier();


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

  partition_list(grph_list,n_vertices,&my_start,&my_end,TH);    
  node_Barrier();
  	
  buff = node_malloc(sizeof(int)*n_vertices,TH);
  node_Barrier();
  pardo(i,0,n_vertices,1) buff[i]=0;
 
  node_Barrier();
  
  pardo(i,0,n_vertices,1)
  {
  	buff[grph_list[i].wk_space1]=1;
  }
 
  
  node_Barrier();
  
#if 0
  
  on_one_thread {
  	for(i=0;i<n_vertices;i++) printf("%d ",buff[i]);
	printf("\n");
  }
 on_one_thread {
  	for(i=0;i<n_vertices;i++) printf("%d ",grph_list[i].wk_space1);
	printf("\n");
  }
#endif
  
  prefix_sum(buff,n_vertices,TH);
  node_Barrier();

#if 0
  on_one_thread {
  	for(i=0;i<n_vertices;i++) printf("%d ",buff[i]);
	printf("\n");
  }
#endif
  
  new_n_vertices=buff[n_vertices-1];
  on_one_thread printf("The number of new vertices is %d\n",new_n_vertices);
  tmp_list=node_malloc(sizeof(ent_t)*new_n_vertices,TH);  
  
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
		/*now I have k consequtive nodes starting from i to be merged*/
		/*The number of adj elements for these nodes is n*/
		left.list=i;
		left.element=0;
		right.list=i+k-1;
  		right.element=grph_list[right.list].n_neighbors-1;	

#if PROFILING
		s1=gethrtime();
#endif
		
		Qsort(grph_list,left,right);
		
#if PROFILING	
  		t1=gethrtime();
  		interval=t1-s1;
  		interval=interval/NANO;  
  		sort_time[MYTHREAD]+=interval;
#endif  


#if 0
		check_sort(grph_list,left,right);
#endif		

		new_len = n_distinct(grph_list,left,right,key);
		
		new_ele_arr = malloc(sizeof(ele_t)*new_len);
		compact(new_ele_arr, grph_list, left, right,buff,key);
	
		d=grph_list[i].wk_space1;
		d=buff[d]-1;

#if 0
		printf("I am setting the %d's entry of the new list\n",d);	
#endif

		tmp_list[d].my_neighbors=new_ele_arr;
		tmp_list[d].n_neighbors=new_len;
		i=j;
	}
  node_Barrier();
  
  pardo(i,0,n_vertices,1)
  	free(grph_list[i].my_neighbors);
  node_Barrier();

#if PROFILING
  end=gethrtime();
  interval=end-start;
  interval=interval/NANO;  
  compact_list_time[MYTHREAD]+=interval;
#endif
  
  *p_n_vertices=new_n_vertices;
  node_free(grph_list,TH);
  node_free(buff,TH);
  return(tmp_list);	
  
}



/*This function will use a different approach to compact the list.
  Not using qsort for the entries for each d, but use additional memory
  */

ent_t * compact_adj_list_2 (ent_t * grph_list,int *p_n_vertices,int * buff1,THREADED)
{
  int my_local_entries=0,my_start,my_end,new_len,n_vertices,new_n_vertices;
  int n,i,j,k,d,l,v,w,key,min_v,max_v;
  int * buff;
  index_t_ left,right,q,ind;
  ele_t * new_ele_arr; 
  ent_t * tmp_list;

#if PROFILING 
  hrtime_t start,end,s1,t1;
  double interval;
  start=gethrtime();
#endif
       
  n_vertices=*p_n_vertices;
  tmp_list = node_malloc(sizeof(ent_t)*n_vertices,TH);
  all_radixsort_smp_s2(n_vertices,grph_list,tmp_list,TH);
  node_free(grph_list,TH);
  grph_list = tmp_list;
  node_Barrier();

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
 
  partition_list(grph_list,n_vertices,&my_start,&my_end,TH);     
  node_Barrier();
  	
  buff = node_malloc(sizeof(int)*n_vertices,TH);
  node_Barrier();
  pardo(i,0,n_vertices,1) buff[i]=0;
 
  node_Barrier();
  
  pardo(i,0,n_vertices,1)
  {
  	buff[grph_list[i].wk_space1]=1;
  }
  
  node_Barrier(); 
  
  prefix_sum(buff,n_vertices,TH);
  node_Barrier();
  
  new_n_vertices=buff[n_vertices-1];
  tmp_list=node_malloc(sizeof(ent_t)*new_n_vertices,TH);  
  
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
		/*now I have k consequtive nodes starting from i to be merged*/
		/*The number of adj elements for these nodes is n*/
		left.list=i;
		left.element=0;
		right.list=i+k-1;
  		right.element=grph_list[right.list].n_neighbors-1;	

		get_maxmin(grph_list,left,right,&min_v,&max_v);

#if 0		
		printf("min_v=%d, max_v=%d, number of vertices:%d\n",min_v,max_v,n);
#endif

	 	d = buff[max_v]-buff[min_v]+1;			
#if 0
		printf("buff[max_v]=%d,buff[min_v]=%d,d is %d\n",d,buff[max_v],buff[min_v]);
#endif

		for(l=0;l<d;l++)
			buff1[l]=-1;
	
		new_len=0;	
		
		for(ind=left; leq_(ind,right); incr_(grph_list,&ind))
		{
			
			v=V(grph_list,ind);
			if(v==key) continue;
				
			w=W(grph_list,ind);
			v=buff[v];
			v=v-buff[min_v];
			if(buff1[v]==-1 ) {
				buff1[v] = w;
				new_len++;
			} else if (buff1[v]>w) buff1[v]=w; 
		
		}	
#if 0
		printf("merging done\n");
#endif
		new_ele_arr = malloc(sizeof(ele_t)*new_len);
		k=0;
		for(l=0;l<d;l++)
		{
			if(buff1[l]!=-1){
				new_ele_arr[k].v=buff[min_v]+l-1;
				new_ele_arr[k++].w=buff1[l];
			}
		}				
		if(k!=new_len|| new_len==0) printf(" k=%d, new_len=%d,key is %d\n",k,new_len,key);
		
		d=grph_list[i].wk_space1;
		d=buff[d]-1;
#if 0
		printf("I am setting the %d's entry of the new list\n",d);	
#endif

		tmp_list[d].my_neighbors=new_ele_arr;
		tmp_list[d].n_neighbors=new_len;
		i=j;
	}
  node_Barrier();
 
#if 0  
  pardo(i,0,new_n_vertices,1)
  {
  	printf("%d: ",i);
  	for(k=0;k<tmp_list[i].n_neighbors;k++)
		printf("(%d,%d) ", tmp_list[i].my_neighbors[k].v,tmp_list[i].my_neighbors[k].w);
	printf("\n");
  }
#endif

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
  printf("Metrics:This round:compact_list_2 time is %f s\n",interval);
#endif
  
  *p_n_vertices=new_n_vertices;
  node_free(grph_list,TH);
  node_free(buff,TH);
  return(tmp_list);	
  
}
int get_maxmin(ent_t * grph_list,index_t_ start, index_t_ end, int * min_v, int * max_v)
{
	index_t_ i;
	int min,max;

	min=MAXINT;
	max=0;
	for(i=start; leq_(i,end); incr_(grph_list,&i))
	{
		if(V(grph_list,i)>max) max=V(grph_list,i);
		if(V(grph_list,i)<min) min=V(grph_list,i);
	}
 	*min_v=min;
	*max_v=max;	
}



/*compact_adj_list_2 does not scale well. Try to make it more cache friendly*/

ent_t * compact_adj_list_3 (ent_t * grph_list,int *p_n_vertices,THREADED)
{
  int my_local_entries=0,my_start,my_end,new_len,n_vertices,new_n_vertices;
  int n,m,i,j,k,d,l,v,w,key,min_v,max_v,v1,offset,base,rounds;
  int * buff,*buff1;
  index_t_ left,right,q,ind;
  ele_t * new_ele_arr; 
  ent_t * tmp_list;

#if PROFILING 
  hrtime_t start,end,s1,t1;
  double interval;
  start=gethrtime();
#endif
       
  n_vertices=*p_n_vertices;
  tmp_list = node_malloc(sizeof(ent_t)*n_vertices,TH);
  all_radixsort_smp_s2(n_vertices,grph_list,tmp_list,TH);
  node_free(grph_list,TH);
  grph_list = tmp_list;
  node_Barrier();

#if 0
  on_one_thread {
  	for(i=1;i<n_vertices;i++)
		if(KEY(grph_list[i])<KEY(grph_list[i-1])) printf("Sorting error\n");
  }  
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

  partition_list(grph_list,n_vertices,&my_start,&my_end,TH);    
  node_Barrier();
  	
  buff = node_malloc(sizeof(int)*n_vertices,TH);
  node_Barrier();
  pardo(i,0,n_vertices,1) buff[i]=0;
 
  node_Barrier();
  
  pardo(i,0,n_vertices,1)
  {
  	buff[grph_list[i].wk_space1]=1;
  }
  
  node_Barrier(); 
  
  prefix_sum(buff,n_vertices,TH);
  node_Barrier();
  
  new_n_vertices=buff[n_vertices-1];
  buff1 = malloc(sizeof(int)*NCACHE_INTS);
  for(l=0;l<NCACHE_INTS;l++)
			buff1[l]=-1;
			
  on_one_thread printf("The number of new vertices is %d\n",new_n_vertices);
  tmp_list=node_malloc(sizeof(ent_t)*new_n_vertices,TH);  
  
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
		/*now I have k consequtive nodes starting from i to be merged*/
		/*The number of adj elements for these nodes is n*/
		left.list=i;
		left.element=0;
		right.list=i+k-1;
  		right.element=grph_list[right.list].n_neighbors-1;	

		get_maxmin(grph_list,left,right,&min_v,&max_v);

#if 0		
		printf("buff[min_v]=%d, buff[max_v]=%d, number of vertices:%d\n",buff[min_v],buff[max_v],n);
#endif
	 	d = buff[max_v]-buff[min_v]+1;				
	
				
		new_ele_arr = malloc(sizeof(ele_t)*d);				
		rounds = d/NCACHE_INTS;
		if(d%NCACHE_INTS!=0) rounds++;

		base=0;
		offset=NCACHE_INTS;
		k=0;
		new_len=0;
#if 0
		printf(" need %d rounds\n",rounds);
#endif
				
		for(m=0;m<rounds;m++)
		{								
			 for(ind=left; leq_(ind,right); incr_(grph_list,&ind))
			 {

				 v=V(grph_list,ind);
				 if(v==key) continue;
				 v=buff[v];
				 v=v-buff[min_v];
				 if(v>=offset || v<base) continue;

				 w=W(grph_list,ind);						 
				 v1=v-base;

				 if(buff1[v1]==-1 ) {
					 buff1[v1] = w;
					 new_len++;
				 } else if (buff1[v1]>w) buff1[v1]=w; 

			 }	

			 for(l=0;l<NCACHE_INTS;l++)
			 {
				 if(buff1[l]!=-1){
					 new_ele_arr[k].v=base+buff[min_v]+l-1;
					 new_ele_arr[k++].w=buff1[l];
					 buff1[l]=-1;
				 }
			 }
			 base=offset;
			 offset+=NCACHE_INTS;
		}
				
		new_ele_arr=realloc(new_ele_arr,sizeof(ele_t)*k);
		if(new_len!=k || new_len==0) printf("new_len is %d, k is %d\n",new_len,k);
				
		d=grph_list[i].wk_space1;
		d=buff[d]-1;
#if 0
		printf("I am setting the %d's entry of the new list\n",d);	
#endif

		tmp_list[d].my_neighbors=new_ele_arr;
		tmp_list[d].n_neighbors=k;
		i=j;
	}
  node_Barrier();
 
#if 0  
  pardo(i,0,new_n_vertices,1)
  {
  	printf("%d: ",i);
  	for(k=0;k<tmp_list[i].n_neighbors;k++)
		printf("(%d,%d) ", tmp_list[i].my_neighbors[k].v,tmp_list[i].my_neighbors[k].w);
	printf("\n");
  }
#endif

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
  printf("Metrics:This round:compact_list_3 time is %f s\n",interval);
#endif
  
  *p_n_vertices=new_n_vertices;
  node_free(grph_list,TH);
  node_free(buff,TH);
  return(tmp_list);	
  
}

/* to compact all the elements in grph_list starting from start till end into new_ele_arr*/

void compact(ele_t * new_ele_arr,ent_t * grph_list, index_t_ start, index_t_ end,int *buff,int d)
{
	index_t_ i,j;
	int l=0,v_i,v_j;
  	
	for(i=start; leq_(i,end); incr_(grph_list,&i))
	{
		v_i=V(grph_list,i);
		if(v_i==d) continue;
		
		if(equal_(i,start)){
		 new_ele_arr[l].v=buff[ELEMENT(grph_list,i).v]-1;
		 new_ele_arr[l++].w=ELEMENT(grph_list,i).w;
		}
		else {
			j = i;
			decr_(grph_list,&j);
			v_j=V(grph_list,j);
			if(v_i!=v_j && v_i!=d){
			 	new_ele_arr[l].v=buff[ELEMENT(grph_list,i).v]-1;
		 		new_ele_arr[l++].w=ELEMENT(grph_list,i).w;
			}
		}
	}
	
}


/* get the number of distinct elements in the range of [start,end], and the elements that have
the same v value as d are not counted*/
int n_distinct(ent_t * grph_list, index_t_ start, index_t_ end,int d)
{
	index_t_ i,j;
	int n=0,v_i,v_j;

	for(i=start; leq_(i,end); incr_(grph_list,&i))
	{
		v_i=V(grph_list,i);
		
		if(v_i==d) continue;
		
		if(equal_(i,start)) n++;
		else {
			j = i;
			decr_(grph_list,&j);
			v_j=V(grph_list,j);
			if(v_i!=v_j) n++;
		}
	}
	
	return (n);
	
}

int Bsort(ent_t * grph_list, index_t_ left, index_t_ right)
{
	index_t_ i,j,r;
	ele_t x;
	
	r=right;
	decr_(grph_list,&r);
	
	for(i=left;leq_(i,r);incr_(grph_list,&i))
		for(j=i,incr_(grph_list,&j);leq_(j,right);incr_(grph_list,&j))
		{
				if(V(grph_list,i)>V(grph_list,j) || V(grph_list,i)==V(grph_list,j) && W(grph_list,i)>W(grph_list,j))
				{
					x=ELEMENT(grph_list,i);
					ELEMENT(grph_list,i)=ELEMENT(grph_list,j);
					ELEMENT(grph_list,j)=x;	
				}
		}
}

/*When sorting, the elements in the list should be in their D(i) values*/
int Qsort(ent_t *grph_list, index_t_ left, index_t_ right)
{

   index_t_ q;
  
   if(less_(left,right)){
   	q=Partition(grph_list,left,right);
	Qsort(grph_list,left,q);	
	incr_(grph_list,&q);
	Qsort(grph_list,q,right);
	
   }
    
}

index_t_ Partition(ent_t * grph_list,index_t_ p, index_t_ r)
{
	ele_t x,y;
	index_t_ i,j;
	int v,w;
	
	x=ELEMENT(grph_list,p);
	i=p;
	decr_(grph_list,&i);
	j=r;
	incr_(grph_list,&j);
	
	while(1)
	{
		do{
			decr_(grph_list,&j);
#if 0			
			v=V(grph_list,j);
			w=W(grph_list,j);
#endif
			v=grph_list[j.list].my_neighbors[j.element].v;
			w=grph_list[j.list].my_neighbors[j.element].w;			
		}while(!less_(j,p) && (v>x.v || (v==x.v && w>x.w)));
		
		do{
			incr_(grph_list,&i);
		}while(leq_(i,r) && (V(grph_list,i)<x.v || (V(grph_list,i)==x.v && W(grph_list,i)<x.w)));
		
		if(less_(i,j)){
#if 0		
			printf("%d,%d<---->%d,%d\n",i.list,i.element,j.list,j.element);
#endif			
			y=ELEMENT(grph_list,i);
			ELEMENT(grph_list,i)=ELEMENT(grph_list,j);
			ELEMENT(grph_list,j)=y;
		} else return (j);
	}
}


int check_sort(ent_t * grph_list, index_t_ left, index_t_ right)
{
	int wrong = 0;
	index_t_ i,j;
	
	i=left;
	while(less_(i,right))
	{	
		j=i;
		incr_(grph_list,&j);
#if 0
		printf(" (%d,%d):V=%d, W=%d  ", i.list,i.element,V(grph_list,i),W(grph_list,i));
#endif
		
		if(V(grph_list,i)> V(grph_list,j) || (V(grph_list,i)== V(grph_list,j) && W(grph_list,i)>W(grph_list,j))) 
		{
			wrong =1;
			break;
			
			printf("Sorting results are wrong:\n");
			printf(" (%d,%d):V=%d, W=%d  ", i.list,i.element,V(grph_list,i),W(grph_list,i));
			printf(" (%d,%d):V=%d, W=%d  ", j.list,j.element,V(grph_list,j),W(grph_list,j));

		}

		i=j;
	}

#if 0	
	if(wrong==1)
	{
		i=left;
		while(less_(i,right))
		{	
			j=i;
			incr_(grph_list,&j);
			printf("(%d,%d):V=%d, W=%d \n",i.list,i.element,V(grph_list,i),W(grph_list,i));		
			i=j;
		}
		printf("\n done\n");
	}
#endif
	
}


inline void incr_(ent_t * grph_list, index_t_ *ind)
{
	if(ind->list==-1 && ind->element==-1) {
		ind->list=0;
		ind->element=0;
	}else{
		if(grph_list[ind->list].n_neighbors==ind->element+1){
			ind->list++;
			ind->element=0;
		} else ind->element++;
	}
}

inline void decr_(ent_t * grph_list, index_t_ *ind)
{
	if(ind->list==0 && ind->element==0) {
		ind->list=-1;
		ind->element=-1;
	}else {
		if(ind->element==0){
			ind->list--;
			ind->element=grph_list[ind->list].n_neighbors-1;
		} else ind->element--;
	}
#if 0
	if(ind->list<0 || ind->element<0) 
		printf("Error in decr_:(list,element)==(%d,%d)\n",ind->list,ind->element);
#endif

}

inline int less_(index_t_ ind_l, index_t_ ind_r)
{
#if 0
	printf("(ind_l.list,ind_l.element)==(%d,%d)\n",ind_l.list,ind_l.element);
	printf("(ind_r.list,ind_r.element)==(%d,%d)\n",ind_r.list,ind_r.element);
#endif

	if(ind_l.list<ind_r.list || (ind_l.list==ind_r.list && ind_l.element<ind_r.element))
	return 1;
	else return 0;  
}

inline int leq_(index_t_ ind_l, index_t_ ind_r)
{
#if 0
	printf("(ind_l.list,ind_l.element)==(%d,%d)\n",ind_l.list,ind_l.element);
	printf("(ind_r.list,ind_r.element)==(%d,%d)\n",ind_r.list,ind_r.element);
#endif

	if(ind_l.list<ind_r.list || (ind_l.list==ind_r.list && ind_l.element<=ind_r.element))
	return 1;
	else return 0;  
}

inline int equal_(index_t_ i, index_t_ j)
{

	if(i.list==j.list && i.element==j.element) return 1;
	else return 0;
}

int partition_list(ent_t * grph_list, int n_vertices, int * mystart, int *myend,THREADED)
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
