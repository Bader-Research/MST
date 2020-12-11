#include "simple.h"
#include "../grph.h"
#include <values.h>

/*instead of segment the list into different parts to give to differtn
  processors, all processors look at the same list, and each has a 
  part of the list to look into. Too many barrier operations occured. 
  Very slow */
int Boruvka_sparse_2(ent_t *grph_list, int n_vertices,THREADED)
{
  hrtime_t start,end;
  double interval,interval1=0,interval2=0,interval3=0;

  int *Min, *Min_ind,newly_dead=0,*p_alive,*p_total,*live,*D,*dead_matrix,*total_matrix;
  int k,l,w,i,j,t,ind,total=0;

  Min = node_malloc(sizeof(int)*THREADS,TH);
  Min_ind = node_malloc(sizeof(int)*THREADS,TH);
  D = node_malloc(sizeof(int)*n_vertices,TH);
  live = node_malloc(sizeof(int)*n_vertices,TH);
  dead_matrix = node_malloc(sizeof(int)*THREADS,TH);
  total_matrix = node_malloc(sizeof(int)*THREADS,TH);
  p_alive = node_malloc(sizeof(int),TH);
  p_total = node_malloc(sizeof(int),TH);

  on_one_thread printf("Boruvka_sparse_2\n");
  on_one_thread {
	*p_alive = n_vertices;
	*p_total = 0;
		
  }
  pardo(i,0,n_vertices,1){
	 live[i]=1;
	 D[i]=i;
  }

  node_Barrier();

  while(1) { 
 	start = gethrtime(); 
	newly_dead=0;
	total=0;

	node_Barrier();

	for(i=0;i<n_vertices;i++)
	{
		t=MAXINT;
		pardo(k,0,grph_list[i].n_neighbors,1)
          	{
            		j=grph_list[i].my_neighbors[k].v;
            		j=D[j];
            		if(D[i]!=j && t>grph_list[i].my_neighbors[k].w ){
                 		t = grph_list[i].my_neighbors[k].w;
                 		ind = j;
			}
             	}
		Min[MYTHREAD]=t;
		Min_ind[MYTHREAD]=ind;
		node_Barrier();
		on_one_thread {
			for(k=1;k<THREADS;k++)
				if(Min[k]<t) {
					t=Min[k];
					ind=Min_ind[k];
				}
			D[D[i]]=ind;
		}
		
		node_Barrier(); 
          }
	node_Barrier();
	printf("after grafting phase 1 \n");

	pardo(i,0,n_vertices,1)
	{
          if(!live[i]) continue;
          if(D[D[i]]!=i|| (D[D[i]]==i && i<D[i]) ){
                 total+=Min[i];
#if 0
                 printf("<%d,%d>\n",i,D[i]);
#endif
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

	if(*p_alive==1) break;

	node_Barrier();
	pardo(i,0,n_vertices,1)
		if(i==D[D[i]] && i<D[i]) D[i]=i;
	node_Barrier();

	printf("Before pointer jumping\n");
	pardo(i,0,n_vertices,1)
	{
                while(D[i]!=D[D[i]]) D[i]=D[D[i]]; /*pointer-jumping to update n
ewest super-vertex for all*/
                if(D[i]!=i && live[i])
                        live[i]=0;

	}
	node_Barrier();
	end=gethrtime();
	interval = end - start;
	on_one_thread printf(" Alive vertices:%d, total weights:%d,time used in this round :%f s\n",*p_alive,*p_total,interval/1000000000); 

  } /*while*/

  printf("The total weight is %d\n",*p_total);
  node_Barrier();

  node_free(p_alive,TH); 
  node_free(p_total,TH); 
  node_free(dead_matrix,TH);
  node_free(total_matrix,TH);
  node_free(D,TH);
  node_free(Min,TH);
  node_free(Min_ind,TH);
}

/* 
 Same as _2. Except:Use Boruvka steps to shrink the number of vertices, then use a dense representation, very slow though because of too many barriers*/
int Boruvka_sparse_3(ent_t *grph_list, int n_vertices,THREADED)
{
#define THRES 2048 
  hrtime_t start,end;
  double interval1=0,interval2=0,interval3=0;

  int *label,*Min, *Min_ind,newly_dead=0,*p_alive,*p_total,*live,*D,*dead_matrix,*total_matrix;
  int x,y,k,l,w,i,j,n,total=0;
  int ** grph_matrix,**tmp_matrix,***dense_M;

  Min = node_malloc(sizeof(int)*n_vertices,TH);
  Min_ind = node_malloc(sizeof(int)*n_vertices,TH);
  D = node_malloc(sizeof(int)*n_vertices,TH);
  live = node_malloc(sizeof(int)*n_vertices,TH);
  dead_matrix = node_malloc(sizeof(int)*THREADS,TH);
  total_matrix = node_malloc(sizeof(int)*THREADS,TH);
  p_alive = node_malloc(sizeof(int),TH);
  p_total = node_malloc(sizeof(int),TH);
  
  on_one_thread {
	*p_alive = n_vertices;
	*p_total = 0;
		
  }
  pardo(i,0,n_vertices,1){
	 live[i]=1;
	 D[i]=i;
  }

  node_Barrier();

  start = gethrtime();
  while(1) { 
	start = gethrtime();  
	pardo(i,0,n_vertices,1)
        {
		Min[i]=MAXINT;
		Min_ind[i]=-1;
	}
	newly_dead=0;
	total=0;

	node_Barrier();

	for(i=0;i<n_vertices;i++)
	{
		pardo(k,0,grph_list[i].n_neighbors,1)
          	{
            		j=grph_list[i].my_neighbors[k].v;
            		j=D[j];
            		if(D[i]!=j && Min[D[i]]>grph_list[i].my_neighbors[k].w ){
                 		Min[D[i]] = grph_list[i].my_neighbors[k].w;
                 		Min_ind[D[i]]=j;
			}
             	}
          }
	node_Barrier();

	pardo(i,0,n_vertices,1)
		if(Min_ind[i]!=-1) D[i]=Min_ind[i];

	node_Barrier();

	pardo(i,0,n_vertices,1)
	{
          if(!live[i]) continue;
          if(D[D[i]]!=i|| (D[D[i]]==i && i<D[i]) ){
                 total+=Min[i];
#if 0
                 printf("<%d,%d>\n",i,D[i]);
#endif
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

	if(*p_alive==1) break;
        
	node_Barrier();
	pardo(i,0,n_vertices,1)
		if(i==D[D[i]] && i<D[i]) D[i]=i;
	node_Barrier();

	pardo(i,0,n_vertices,1)
	{
                while(D[i]!=D[D[i]]) D[i]=D[D[i]]; /*pointer-jumping to update n
ewest super-vertex for all*/
                if(D[i]!=i && live[i])
                        live[i]=0;

	}
	node_Barrier();
	if(*p_alive <THRES) break;
	end = gethrtime();
	interval1=end-start;
	on_one_thread printf(" Alive vertices:%d, total weights:%d, time for this round:%f s\n",*p_alive,*p_total,interval1/1000000000); 

  } /*while*/

  node_Barrier();
  end = gethrtime();

  interval1=end-start;

  on_one_thread printf("The time needed for spare part is %f s\n",interval1/1000000000);
 
  if(*p_alive!=1) { /*now we convert to a dense representation*/
	start=gethrtime();
	n=*p_alive;
	tmp_matrix = malloc(sizeof(int*)*n);
	for(i=0;i<n;i++) {
		tmp_matrix[i]=malloc(sizeof(int)*n);
		if(tmp_matrix[i]==NULL) printf("ERROR in allocating memories\n");
		for(j=0;j<n;j++) tmp_matrix[i][j]=-1;
	}
	dense_M=node_malloc(sizeof(int**),TH);
	dense_M[MYTHREAD]=tmp_matrix;
	node_Barrier();
	label = node_malloc(sizeof(int)*n_vertices,TH);
	pardo(i,0,n_vertices,1){
	 label[i]=0;
	 label[D[i]]=1;
	}

	node_Barrier();
#if 0
	prefix_sum(label,n_vertices,TH);
#endif
 	on_one_thread {
		for(i=1;i<n_vertices;i++)
			label[i]+=label[i-1];
	}
	node_Barrier();
	printf("Thread <%d> :Prefix sum done\n",MYTHREAD);

	pardo(i,0,n_vertices,1)
        {
		for(k=0;k<grph_list[i].n_neighbors;k++)
		{
			j=grph_list[i].my_neighbors[k].v;
			w=grph_list[i].my_neighbors[k].w;
			x=label[D[i]]-1;
			y=label[D[j]]-1;
			if(x<0 || x>=n || y<0 || y>=n) printf("ERROR label\n");
			if(tmp_matrix[x][y]==-1 || tmp_matrix[x][y]>w)
				tmp_matrix[x][y]=w;
		}	
        
	}

	printf("Thread <%d> entering barrier\n",MYTHREAD);
	node_Barrier();

	printf("Thread<%d> copying out done\n",MYTHREAD);

	grph_matrix=node_malloc(sizeof(int*)*n,TH);
	pardo(i,0,n,1)
	{
		grph_matrix[i]=malloc(sizeof(int)*n);
		for(j=0;j<n;j++) grph_matrix[i][j]=-1;
	}

	node_Barrier();

	pardo(i,0,n,1)
	{
		for(j=0;j<n;j++)
	 	{
		   w=MAXINT;
		   for(k=0;k<THREADS;k++)
			if(w>dense_M[k][i][j]) w=dense_M[k][i][j];
		   grph_matrix[i][j]=w;
		}
	}
 	node_Barrier();

	for(i=0;i<n;i++) free(tmp_matrix[i]);
	free(tmp_matrix);
	Boruvka_dense(grph_matrix,n,TH);
	end = gethrtime();
	interval1=end-start;
	on_one_thread printf("The time used to do the transformation and dense part is %f s\n",interval1/1000000000);
   }/* *p_alive!=1*/
	
  printf("The total weight is %d\n",*p_total);
  node_Barrier();

  node_free(p_alive,TH); 
  node_free(p_total,TH); 
  node_free(dead_matrix,TH);
  node_free(total_matrix,TH);
  node_free(D,TH);
  node_free(Min,TH);
  node_free(Min_ind,TH);
}
