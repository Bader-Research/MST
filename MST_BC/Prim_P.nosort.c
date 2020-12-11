#include "simple.h"
#include "../grph.h"
#include <sys/types.h>
#include <values.h>
#include "my_type.h"
#include "my_heap.h"
#include "my_malloc.h"
#include "merge.h"
#include "prefix_sum.h"


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

/* In this version, lets sort the vertices according to the degrees, and start from the lowest degree*/
int Prim_Par(ent_t *grph_list, int n_vertices,int m,THREADED)
{
#define PRE_COLOR -1

	int i,j,k,w,n1,n2,n3,n,position,u,v,change,l,my_color,min_ind1,min_ind2,min1,min2;
	int total,n_visited,* heap_pointer,*color,*visited,*D,*T,heap_count,bad_count,b_break;
	int break_vertex,max,s,t,min,ind;
	heap_t heap;
	edge_t *local_list,*inter_list;
	ele_t *tmp_arr,*sort_buff;
	int ** grph_matrix;
	q_ele_t  el;
	hrtime_t start,end;  	
	double interval;
	
	start = gethrtime();
	
	
	max=0;
	v=0;
	color=node_malloc(sizeof(int)*n_vertices,TH);
	visited=node_malloc(sizeof(int)*n_vertices,TH);
	pardo(i,0,n_vertices,1){
	 color[i]=0;
	 visited[i]=0;
	}	
	local_list =(edge_t *) my_malloc(sizeof(edge_t)*(2*m/THREADS),TH);
	inter_list =(edge_t *) my_malloc(sizeof(edge_t)*(2*m/THREADS),TH); 
	tmp_arr = my_malloc(sizeof(ele_t)*(n_vertices),TH);
	sort_buff =my_malloc(sizeof(ele_t)*(n_vertices),TH);	
	
	heap_pointer = (int *)my_malloc(sizeof(int)*n_vertices,TH);
	for(i=0;i<n_vertices;i++) heap_pointer[i]=-1;
	
	heap=create_heap(n_vertices,heap_pointer,greater,less);
		
	n1=0;
	n2=0;	
	n=0;
	n3=0;
	total=0;
	n_visited=0;
	
	bad_count=0;		
/*	pardo(i,0,n_vertices,1) */
	pardo(i,n_vertices-1,-1,-1)
	{
		if(color[i]!=0) continue;
		my_color=n*THREADS+MYTHREAD+1;	
		color[i]=my_color;
		n++;     /*count how many components I got*/
		
		el.key=0;
     	el.v=i;
		el.u=i;
					
     	position=heap_insert(heap,el);
	 	heap_pointer[el.v]=position;

		break_vertex=-1;		
#if 0 
		/* Anyway I will find min for i */
		min1=MAXINT;
	 	for(k=0;k<grph_list[i].n_neighbors;k++)
		{
			if(grph_list[i].my_neighbors[k].w<min1) {
				min1 = grph_list[i].my_neighbors[k].w;
				min_ind1 = k;
			}
		}
		local_list[n1].v1=i;
		local_list[n1].v2=grph_list[i].my_neighbors[min_ind1].v;
		local_list[n1++].w=min1;
#endif
	
     		while (!heap_is_empty(heap))
     		{
        		heap_extract_min(heap,&el);
        		v=el.v;
				heap_pointer[v]=-1;
			if(color[v]!=my_color){
				break_vertex=el.u;
				break;
			}
			b_break=0;
		    for(k=0;k<grph_list[v].n_neighbors;k++)
			{
				if(color[grph_list[v].my_neighbors[k].v]!=0 && color[grph_list[v].my_neighbors[k].v]!=my_color)
				{
					b_break=1;
					break;
				}
			}
			if(b_break==1) {
				break_vertex=el.u;
				break;		
			}
			if(visited[v]) continue;
			
        		total+=el.key;	
			visited[v]=1;
			local_list[n1].v1=el.v;
			local_list[n1].v2=el.u;
			local_list[n1].w=el.key;
			n1++;
			/*printf("THREAD %d: <%d,%d,%d>\n",MYTHREAD,el.v,el.u,el.key);*/
					
        		for(k=0;k<grph_list[v].n_neighbors;k++)
        		{
				u=grph_list[v].my_neighbors[k].v;
				w=grph_list[v].my_neighbors[k].w;
				{
					if(visited[u] && color[u]==my_color) continue;
					if (color[u]==0) color[u]=my_color;
					el.v=u;
		   	  		el.key=w;
					el.u=v;					
		   	  		if(heap_pointer[u]==-1)
			  		{		
              					position=heap_insert(heap,el);
						heap_pointer[u]=position;
			  		} else {
			  			position=heap_decrease_key(heap,heap_pointer[el.v],el);
						heap_pointer[el.v]=position;
			  		}
            	} 
				
         	} 	/*for*/
      	} 	/*while*/
		
		 /*heap_clean(heap);
		 for(i=0;i<n_vertices;i++) heap_pointer[i]=-1;*/
		 	
		 while (!heap_is_empty(heap))
     		{
        		heap_extract_min(heap,&el);
        		v=el.v;
				heap_pointer[v]=-1;
			}	
	} 	/*pardo*/
	node_Barrier();
printf(" n1 is %d \n",n1);
#if 1
	pardo(i,0,n_vertices,1){
		if(visited[i]==0) {
			min1 = MAXINT;
			for(j=0;j<grph_list[i].n_neighbors;j++)
			{
				w=grph_list[i].my_neighbors[j].w;
				if(min1 > w){
				 min1 = w;
				 min_ind1 = grph_list[i].my_neighbors[j].v;
				}
			}
#if 1 
/* only for debugging purpose we will calculate the total weights. */
/* and a possible bug here is that min_ind2 might not be the same as i because
 of edges have the same weight, yet min_ind2 is in effect i*/

			min2=MAXINT;
			for(j=0;j<grph_list[min_ind1].n_neighbors;j++)
			{
				w = grph_list[min_ind1].my_neighbors[j].w;				
				if(min2>w)
				{
					min2 = w;
					min_ind2=grph_list[min_ind1].my_neighbors[j].v;
				}
			}
			if(visited[min_ind1] || min_ind2!=i || (min_ind2==i && i<min_ind1)) total+=min1;
#endif			
			
			local_list[n1].v1=i;
			local_list[n1].v2=min_ind1;
			local_list[n1++].w=min1;
		}
	}
#endif

	end = gethrtime();
	interval=end-start;
	interval=interval/NANO;
	printf("THREAD %d:time %f s, n=%d ,n1=%d, n2=%d,total=%d,bad_count=%d\n",MYTHREAD,interval,n,n1,n2,total,bad_count);
	node_Barrier();
		
	n=node_Reduce_i(n,SUM,TH);
	on_one_thread printf(" n is %d\n",n);
	
	
	D=color;
	T=visited;
	pardo(i,0,n_vertices,1) D[i]=i;
	node_Barrier();
	
	change = 1;
    	while(change) {
        change=0;
        for(l=0;l<n1;l++)
        {   
		i=local_list[l].v1;
			j=local_list[l].v2;          
            if(D[i]==D[D[i]] && D[j]<D[i]){
				D[D[i]]=D[j];
            	change=1;
             }
			else if(D[j]==D[D[j]] && D[j]>D[i]){
				D[D[j]]=D[i];
            	change=1;
             } 
                
        }
        node_Barrier();

        pardo(i,0,n_vertices,1)
             while(D[i]!=D[D[i]]) D[i]=D[D[i]];
			 
		change=node_Reduce_i(change,MAX,TH);
	}	

	node_Barrier();
	pardo(i,0,n_vertices,1){
		if(D[i]==i) T[i]=1;
		else T[i]=0;
	}	
	node_Barrier();
	
	prefix_sum(T,n_vertices,TH);	
	node_Barrier();
	total=node_Reduce_i(total,SUM,TH);
		
	on_one_thread printf("The number of components i got is %d\n", T[n_vertices-1]);

	if(T[n_vertices-1]>4096) {
		
		 pardo(i,0,n_vertices,1){
			 grph_list[i].wk_space1=D[i];
			 for(j=0;j<grph_list[i].n_neighbors;j++)
                        	 grph_list[i].my_neighbors[j].v=D[grph_list[i].my_neighbors[j].v];
		 }
		 node_Barrier();


		 grph_list=compact_adj_list_arr(grph_list,&n_vertices,tmp_arr,sort_buff,TH);			
		 node_Barrier();

		 on_one printf("compact done\n");
		 Boruvka_sparse_merge(grph_list,n_vertices,TH);
		 on_one printf("after compact\n");
   } else {
   
		printf("greater than 4096\n");
   		n=T[n_vertices-1];
		grph_matrix = node_malloc(sizeof(int*)*n,TH);
		pardo(i,0,n,1){
		grph_matrix[i]=malloc(sizeof(int)*n);
		for(j=0;j<n;j++)
			grph_matrix[i][j]=-1;
		}
	
		node_Barrier();
		n2=0;
		pardo(i,0,n_vertices,1)
		{
			for(k=0;k<grph_list[i].n_neighbors;k++)
			{
				j=grph_list[i].my_neighbors[k].v;
				if(D[i]==D[j]) continue;
				else {
					inter_list[n2].v1=D[i];
					inter_list[n2].v2=D[j];
					inter_list[n2].w=grph_list[i].my_neighbors[k].w;
					n2++;
				}		
			}
		}		
		node_Barrier();
	
		for(k=0;k<THREADS;k++)
		{
			if(MYTHREAD==k){	
			for(l=0;l<n2;l++)
			{
				i=inter_list[l].v1;
				j=inter_list[l].v2;
				i=T[D[i]]-1;
				j=T[D[j]]-1;	
				if(grph_matrix[i][j]==-1 || grph_matrix[i][j]>inter_list[l].w){
					grph_matrix[i][j]=inter_list[l].w;
					grph_matrix[j][i]=inter_list[l].w;
				}
			}
			}
			node_Barrier();
		}	
		node_Barrier();
		
		on_one_thread{
		  int n1=0;
		  int total1=0;
		  el.key=0;
    	  el.v=0;

		  n_vertices=n;

  		 for(i=0;i<n_vertices;i++){
  		  visited[i]=0;
   		 heap_pointer[i]=-1;
   		 }

  		 heap=create_heap(n_vertices,heap_pointer,greater,less);

    	 heap_insert(heap,el);    

    	  while (!heap_is_empty(heap) && n1<n_vertices)
    	  {
        	 heap_extract_min(heap,&el);
        	 v=el.v;
			 if(visited[v]) continue;
			 total1+=el.key;
			 visited[v]=1;
        	 n1++;
        	 for(i=0;i<n_vertices;i++)
        	 {
	   			 if(grph_matrix[v][i]!=-1 && visited[i]==0) {
	      			 el.key=grph_matrix[v][i];
 	      			 el.v=i;
					 if(heap_pointer[i]==-1) heap_insert(heap,el);
					 else heap_decrease_key(heap,heap_pointer[i],el);
            	 } 
        	  }/*for */ 
    	   }/*while*/

    	   printf("Seq Prim: total weights is %d, and total is %d \n",total1,total+total1);
		  }
    }	
	node_Barrier();

}


#if 0

void Prim_Par_MST(ent_t *grph_list, int n_vertices,int m,THREADED)
{
	int i,j;
	hrtime_t t1,s1;
		
	
	while(n_vertices!=1) {
	
		Prim_Par(grph_list, n_vertices,m,TH);
			
		pardo(i,0,n_vertices,1)
		{
			grph_list[i].wk_space1=D[i];
			for(j=0;j<grph_list[i].n_neighbors;j++)
				grph_list[i].my_neighbors[j].v=D[grph_list[i].my_neighbors[j].v];
		}
	
		node_Barrier();
		t1 = gethrtime();
		interval = t1-s1;
		interval = interval /NANO;
		ti_cc += interval;
		s1 = gethrtime();
	
		grph_list=compact_adj_list_arr(grph_list,&n_vertices,tmp_arr,TH);
		if(n_vertices<=1) break;
	}
	
}
#endif
