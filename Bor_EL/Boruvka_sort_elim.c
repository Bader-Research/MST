#include "simple.h"
#include "../grph.h"
#include <sys/types.h>
#include <values.h>
#include "types.h"
#include "my_heap.h"

/* The graph is represented as edge list, each each appears twice in the list
	For each phase,  the edeg_list is sorted by the following keys:
	1. D[v1] as primary key
	2. D[v2] as tiertiary key
	Start[i] and End[i] are the indices to the edge_list where vertex i's edge list starts
*/

inline int greater(q_ele_t a, q_ele_t b)
{
        if(a.key>b.key) return(1);
        else return(0);
}

inline int less(q_ele_t a, q_ele_t b)
{
        if(a.key<b.key) return(1);
        else return(0);
}


#define DEBUG 1	
int * D;	
TYPE* Out,**out_array_ptr;
edge_t _my_max,_my_max_m1;

#if 1
/* This is used to eliminate edges that are sure to be not in the MST */
int Boruvka_elim(edge_t ** edge_list, int * Start, int * End, int n_vertices,int n_edges,int n_Prim_runs,THREADED)
{
	int * R,*D,i,j,k,u,v,*heap_pointer,position,max=0,heap_size;
	heap_t heap;
	q_ele_t el;
	hrtime_t t_start,t_end;
	double interval;
	
	pardo(i,0,n_vertices,1)
		if(max< End[i]-Start[i]) max = End[i]-Start[i];
	max = node_Reduce_i(max,MAX,TH);
		
	heap_size = min(n_vertices,(n_Prim_runs+1)*max);
	R = node_malloc(sizeof(int)*n_edges,TH);
	D = node_malloc(sizeof(int)*n_vertices,TH);
	pardo(i,0,n_vertices,1)
		D[i]=i;
	pardo(i,0,n_edges,1)
		R[i]=0;
	node_Barrier();
	
	heap_pointer = malloc(sizeof(int)*heap_size);
	for(i=0;i<heap_size;i++) heap_pointer[i]=-1;
    heap=create_heap(heap_size,heap_pointer,greater,less);

	t_start = gethrtime();
	
	pardo(i,0,n_vertices,1)
	{
		el.key=0;
		el.v=i;
		el.u=-1;
    	position=heap_insert(heap,el);
    	heap_pointer[el.v]=position;
		k=0;
    	while (!heap_is_empty(heap) && k++<n_Prim_runs)
    	{
        	heap_extract_min(heap,&el);
			v=el.v;
			u=el.u;
			if(u!=-1){
				R[u]=1;
				if(v>edge_list[u]->v1)
					D[v]=edge_list[u]->v1;
				else D[edge_list[u]->v1]=v;
			}
			for(j=Start[v];j<=End[v];j++)
			{
				el.u=j;  /* store j in u */
				el.v=edge_list[j]->v2;
				el.key=edge_list[j]->w;
				heap_insert(heap,el);
			}	
		}
		heap_clean(heap);
	}
	
	node_Barrier();
	t_end = gethrtime();
	interval = t_end - t_start;
	on_one printf("Time used in Prim is %f s\n", interval/1000000000);
	
	pardo(i,0,n_vertices,1)
		while(D[i]!=D[D[i]]) D[i]=D[D[i]];
	node_Barrier();
	printf(" THREAD %d passed pointer jummping \n",MYTHREAD);
	pardo(i,0,n_edges,1)
		if(D[edge_list[i]->v1]!=D[edge_list[i]->v2]) R[i]=1;
	node_Barrier();
	
	prefix_sum(R,n_edges,TH);
	node_Barrier();
	
	on_one printf("number of remaining edges is %d\n", R[n_edges-1]);
	on_one printf("original edges %d\n",n_edges);
	free(heap_pointer);
	node_free(R,TH);
	node_free(D,TH);
	
	
}
#endif

#if 0
int Boruvka_elim(edge_t ** edge_list, int * Start, int * End, int n_vertices,int n_edges,int
n_Prim_runs,THREADED)
{
	int * R,*D,i,j,k,u,v,*heap_pointer,position;
	heap_t heap;
	q_ele_t el;
		
	R = node_malloc(sizeof(int)*n_edges,TH);
	D = node_malloc(sizeof(int)*n_vertices,TH);
	pardo(i,0,n_vertices,1)
		D[i]=i;
	pardo(i,0,n_edges,1)
		R[i]=0;
	node_Barrier();
	
	heap_pointer = malloc(sizeof(int)*(n_vertices));
	for(i=0;i<n_vertices;i++) heap_pointer[i]=-1;
    heap=create_heap(n_vertices,heap_pointer,greater,less);

	for(k=0;k<10;k++)
	{
		i = rand()%n_vertices;
		printf(" i is %d\n",i);
		el.key=0;
		el.v=i;
		el.u=-1;
    	position=heap_insert(heap,el);
    	heap_pointer[el.v]=position;
		k=0;
    	while (!heap_is_empty(heap) && k++<n_Prim_runs/10)
    	{
        	heap_extract_min(heap,&el);
			v=el.v;
			u=el.u;
			if(u!=-1){
				R[u]=1;
				if(v>edge_list[u]->v1)
					D[v]=edge_list[u]->v1;
				else D[edge_list[u]->v1]=v;
			}
			for(j=Start[v];j<=End[v];j++)
			{
				el.u=j;  /* store j in u */
				el.v=edge_list[j]->v2;
				el.key=edge_list[j]->w;
				if(heap_pointer[el.v]==-1)
					heap_insert(heap,el);
				else{
					position=heap_decrease_key(heap,heap_pointer[el.v],el);
					heap_pointer[el.v]=position;
				}
			}	
		}
		for(i=0;i<n_vertices;i++) heap_pointer[i]=-1;
		heap_clean(heap);
	}
	node_Barrier();
	
	pardo(i,0,n_vertices,1)
		while(D[i]!=D[D[i]]) D[i]=D[D[i]];
	node_Barrier();
	
	pardo(i,0,n_edges,1)
		if(D[edge_list[i]->v1]!=D[edge_list[i]->v2]) R[i]=1;
	node_Barrier();
	
	prefix_sum(R,n_edges,TH);
	node_Barrier();

	on_one printf("Initial edges is %d \n", n_edges);	
	on_one printf("number of remaining edges is %d\n", R[n_edges-1]);
	free(heap_pointer);
	node_free(R,TH);
	node_free(D,TH);
	
	
}
#endif

int Boruvka_sort(edge_t ** edge_list, int * Start, int * End, int n_vertices, int n_edges, int n_buf_size,int M, int p, int samples,THREADED)
{
	int i,j, min, index,d,offset,n_edges_l, new_n_edges,new_n_vertices,v1,v2,diff,w_total=0,t_total;
	int * A,*R,*E,size;
	edge_t ** new_edge_list, ** Buffer2;
	
#if DEBUG	
	hrtime_t t_start,t_end,s1,t1;
	double interval,sort_time=0.0, ti_findmin=0, ti_cc=0,ti_compact=0;	
	int * Min;
	Min = node_malloc(sizeof(int)*n_vertices,TH);
	
#endif

	
	D = node_malloc(sizeof(int)*(n_vertices+4),TH);
	R = node_malloc(sizeof(int)*n_vertices,TH);
	A = node_malloc(sizeof(int)*n_edges,TH);		
	Buffer2 = node_malloc(sizeof(edge_t*)*n_buf_size,TH);
	
	node_Barrier();
		
	while(1)
	{
		size = n_edges;
		on_one printf("n_edges is %d, n_vertices is %d\n", n_edges, n_vertices);
		pardo(i,0,n_vertices,1){
		 D[i]=i;
		 R[i]=0;
		}		
		pardo(i,0,n_edges,1){
		 A[i]=0;
		}
		node_Barrier();

		s1 = gethrtime();			
		pardo(i,0,n_vertices,1)
		{
			min = MAXINT;
			index = -1;
			for(j=Start[i];j<=End[i];j++)
				if(edge_list[j]->w < min){
#if DEBUG				
					min=edge_list[j]->w;
#endif					
					index=edge_list[j]->v2; 	
			}
			if(index != -1) {
				D[i]=index;				
				Min[i]=min;
			} 
		}
		node_Barrier();
		t1 = gethrtime();
		interval = t1 - s1;
		interval = interval/NANO;
		ti_findmin+=interval;

#if 0
		on_one printf("***After grafting:\n");
		node_Barrier();
		pardo(i,0,n_vertices,1)
			printf("D[%d]=%d\n", i,D[i]);
#endif
					

		s1 = gethrtime();
		pardo(i,0,n_vertices,1){
			if(D[D[i]]==i && i<D[i]){
			 	D[i]=i;
			}
#if DEBUG			
			 else {
					w_total+=Min[i];
			}
#endif			
		}
		node_Barrier();


#if 0
		on_one printf("After breaking cycles:\n");
		node_Barrier();
		pardo(i,0,n_vertices,1)
			printf("D[%d]=%d\n", i,D[i]);
#endif
		
		
		pardo(i,0,n_vertices,1){
			while(D[i]!=D[D[i]]){
				 D[D[i]]=D[D[D[i]]];
				 D[i]=D[D[i]];
			}
			if(D[i]==i) R[i]=1;
		}
		node_Barrier();

		t1 = gethrtime();
		interval = t1 - s1;
		interval = interval/NANO;
		ti_cc+=interval;
#if 0 
		on_one printf("****After pointer jumpping:\n");
		node_Barrier();
		pardo(i,0,n_vertices,1)
			printf("D[%d]=%d\n", i,D[i]);
#endif

		prefix_sum(R,n_vertices,TH);

#if 0 
		on_one {
			if(n_edges < 50000) {
				printf("The edge list is :\n");		
				for(i=0;i<min(1000,n_edges);i++){
					printf( " {%d,%d,%d}, ", edge_list[i]->v1,edge_list[i]->v2,i);
					if (i%10 ==0) printf("\n");
				}
			}
			printf("\n");
			for(i=0;i<n_vertices;i++)
				printf("D[%d]=%d ", i, D[i]);
		}
		on_one {
			printf("\n");
			if(n_edges < 50000) {
				printf("The edge list is :\n");		
				for(i=0;i<min(1000,n_edges);i++){
					printf( " {%d,%d,%d}, ", D[edge_list[i]->v1],D[edge_list[i]->v2],i);
					if (i%10 ==0) printf("\n");
				}
			}
			printf("\n");
		}
		node_Barrier();
#endif


		s1 = gethrtime();
		on_one {
		/* for set up the MAX that helman's code needs */
			for(i=n_vertices;i<n_vertices+4;i++)
				D[i]=i;
			_my_max.v1 = n_vertices+2;
			_my_max.v2 = n_vertices+2;
			_my_max_m1.v1 = n_vertices+1;
			_my_max_m1.v2 = n_vertices+1;
			init_sort(&_my_max,&_my_max_m1);
			out_array_ptr = &Out;
		}
		node_Barrier();
		
		t_start = gethrtime();
		if(size > 256) {
		master_regular_integer_sort(size,M,p,samples,edge_list,Buffer2,out_array_ptr,TH);
		node_Barrier();			 
		if(abs(Buffer2 - *out_array_ptr) <=1) Buffer2=edge_list;
		edge_list=*out_array_ptr;
		} else 
			_Sort(edge_list, n_edges, D, n_vertices,TH);
		t_end = gethrtime();
		interval = t_end - t_start;
		sort_time +=interval;
		on_one printf("Sorting used time : %f s\n", interval/1000000000);
		
		node_Barrier();
		
#if 0
		on_one {
			printf("The edge list is :\n");		
			for(i=0;i<n_edges;i++)
				printf( " (%d,%d,%d) ", D[edge_list[i]->v1],D[edge_list[i]->v2],edge_list[i]->w);
		}		
		printf("\n");
		node_Barrier();
#endif
#if 0 
		on_one {
			printf("check the sorting result:\n");
			for(i=0;i<n_edges-1;i++)
				if(mygt(edge_list[i],edge_list[i+1])) printf("error \n");
		}
		node_Barrier();
#endif

		new_n_vertices=R[n_vertices-1];

		pardo(i,0,n_edges-1,1)
		{
			if(D[edge_list[i]->v1]!=D[edge_list[i+1]->v1]) {
				Start[R[D[edge_list[i+1]->v1]]-1]=i+1;
				End[R[D[edge_list[i]->v1]]-1]=i;
			}
		}

		on_one {
			Start[R[D[edge_list[0]->v1]]-1]=0;
			End[R[D[edge_list[n_edges-1]->v1]]-1]=n_edges-1;
		}
		
		node_Barrier();

#if 0
		pardo(i,0,new_n_vertices,1)
			printf("Start[%d]=%d, End[%d]=%d\n", i,Start[i],i,End[i]);
#endif

		n_edges_l = 0;
		pardo(i,0,new_n_vertices,1)
		{
			j = Start[i];
			while(j<=End[i])
			{
				min = edge_list[j]->w;
				d = D[edge_list[j]->v2];
				offset=1;
				index=j;
				while(j+offset<=End[i] && D[edge_list[j+offset]->v2]==d)
				{
					A[index]=0;
					if(edge_list[j+offset]->w < min){
						min = edge_list[j+offset]->w;
						index = j+offset;
					}
					offset++;
				}
				if(D[edge_list[index]->v1] != D[edge_list[index]->v2]){
					A[index]=1;
					n_edges_l++;
				}

				j+=offset;
			}
		}

		n_edges_l = node_Reduce_i(n_edges_l,SUM,TH);
		prefix_sum(A,n_edges,TH);
		node_Barrier();

#if 0
		on_one{
			printf("A is :\n");
			for(i=0;i<n_edges;i++)
				printf(" %d ", A[i]);
			printf("\n");
		}
#endif
				
				
		new_n_edges = A[n_edges-1];

#if DEBUG
		t_total = node_Reduce_i(w_total,SUM, TH);
		on_one printf("total is %d\n", t_total);
#endif
		
		if(new_n_edges==0) break;

		//new_edge_list = (edge_t **) node_malloc(sizeof(edge_t *)*new_n_edges,TH);
		new_edge_list =(edge_t **) node_malloc((new_n_edges + 2*(((int) (size/M)) + THREADS*THREADS))
                                                            *sizeof(edge_t*),TH);
		pardo(i,0,n_edges,1)
		{
			if((i==0 && A[i]==1) || (i!=0 && A[i]!=A[i-1]) )
				new_edge_list[A[i]-1]=edge_list[i]; 
				
		}
		
		node_Barrier();
		
		pardo(i,1,new_n_vertices,1){
			diff=A[End[i]]-A[Start[i]];
			if(A[Start[i]]==A[Start[i]-1]){ 
				Start[i]=A[Start[i]];
				diff--;
			}
			else
				 Start[i]=A[Start[i]-1];
			End[i]=max(Start[i],Start[i]+diff);			
		}
		on_one {
			diff=A[End[0]]-A[Start[0]];
			if(A[Start[0]]==0) diff--;
			Start[0]=0;
			End[0]=max(Start[0],Start[0]+diff);
		}

		node_Barrier();		
#if 0
		on_one printf("****After readjusting:\n");
		node_Barrier();
		pardo(i,0,new_n_vertices,1)
			printf("Start[%d]=%d, End[%d]=%d\n", i,Start[i],i,End[i]);
#endif
		
		pardo(i,0,new_n_edges,1){
			v1 = new_edge_list[i]->v1;
			v2 = new_edge_list[i]->v2;
			new_edge_list[i]->v1=R[D[v1]]-1;
			new_edge_list[i]->v2=R[D[v2]]-1;
#if 0
			printf("edge %d: %d,%d,%d\n", i, new_edge_list[i]->v1,new_edge_list[i]->v2,new_edge_list[i]->w);
#endif			
		}
		node_Barrier();
		n_vertices=new_n_vertices;
		n_edges=new_n_edges;	
		node_free(edge_list,TH);
		edge_list=new_edge_list;	
		t1 = gethrtime();
		interval = t1 - s1;
		interval = interval /NANO;
		ti_compact+=interval;	
#if 0
		if(n_edges/n_vertices>2*log2(n_vertices))
			Boruvka_elim(edge_list,Start,End,n_vertices,n_edges,3,TH);
#endif

	}
	
	w_total=node_Reduce_i(w_total,SUM,TH);
	on_one printf("***Total weight is %d\n",w_total);
	on_one printf(" METRICS:Total sorting time is %f s\n", sort_time/1000000000);
	on_one printf(" METRICS: find-min time is %f s, cc time is %f s, compact time is %f s\n", ti_findmin, ti_cc,ti_compact);
	node_free(D,TH);
	node_free(R,TH);
	node_free(A,TH);
}


