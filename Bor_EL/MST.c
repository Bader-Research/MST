#include <sys/types.h>
#include "simple.h"
#include "../grph.h"
#include "values.h"

#define NANO 1000000000
ent_t* grph_list,*tmp;
edge_t **edge_list;
int ** grph_matrix;
int d_or_s;

void *SIMPLE_main(THREADED)
{
  int i,t,j, n_vertices,n_edges,n_buf_size,N,k,max=0,p=8,samples=8,M=8192,n=0;
  hrtime_t start,end;
  double interval,total=0;
  char * input_file;
  int *Start, *End;

  /*initialize graph from input file */  

  input_file = THARGV[0];

  on_one_thread{
	t = read_graph(input_file,&n_vertices,&d_or_s,&grph_list,&grph_matrix);
	if(t!=0) exit(0);
  }
  n_vertices=node_Bcast_i(n_vertices,TH);
  node_Barrier();

  if(d_or_s==DENSE) {
	printf("dense\n");
        start = gethrtime();
  	Boruvka_dense(grph_matrix,n_vertices,TH);
        node_Barrier();
  	end = gethrtime();
  	interval=end-start;
  	on_one_thread printf("METRICS:Boruvka_dense time %f\n",interval/NANO);
  } else {

	int min_d=MAXINT,max_d=0;
	on_one	printf("sparse\n");
	on_one_thread{
		for(i=0;i<n_vertices;i++){
			if(grph_list[i].n_neighbors>max_d) max_d=grph_list[i].n_neighbors;
			if(grph_list[i].n_neighbors<min_d) min_d=grph_list[i].n_neighbors;
			if(grph_list[i].n_neighbors>log2(n_vertices)) n++;
			}
		printf("min_d is %d, max_d is %d, and %d vertices has degree greater than log2(n_vertices)\n", min_d,max_d,n);
	}
#if 0
	start=gethrtime();
	Boruvka_sparse(grph_list,n_vertices,TH);
        node_Barrier();
  	end = gethrtime();
  	interval=end-start;
  	on_one_thread printf("METRICS:Boruvka_sparse time is %f\n",interval/NANO);
	
	start=gethrtime();
	Boruvka_sparse_1(grph_list,n_vertices,TH);
        node_Barrier();
  	end = gethrtime();
  	interval=end-start;
  	on_one_thread printf("METRICS:Boruvka_sparse_1 time is %f\n",interval/NANO);
#endif

#if 0
	start=gethrtime();
	Boruvka_sparse_merge(grph_list,n_vertices,TH);	
 	node_Barrier();
  	end = gethrtime();
  	interval=end-start;
  	on_one_thread printf("METRICS:Boruvka_sparse_merge %f\n",interval/NANO);
#endif

	adj_2_edge_list(grph_list,n_vertices, &edge_list, &n_edges, &n_buf_size,M,TH);
	node_Barrier();
	Start=node_malloc(sizeof(int)*n_vertices,TH);
	End=node_malloc(sizeof(int)*n_vertices,TH);
	pardo(i,0,n_edges-1,1){
		if(edge_list[i]->v1!=edge_list[i+1]->v1) {
			End[edge_list[i]->v1]=i;
			Start[edge_list[i+1]->v1]=i+1;
		}
	}

	Start[0]=0;
	End[n_vertices-1]=n_edges-1;
	start=gethrtime();
	Boruvka_sort(edge_list,Start,End,n_vertices,n_edges,n_buf_size,M,p,samples,TH);

#if 0
	Boruvka_elim(edge_list,Start,End,n_vertices,n_edges,n_vertices/THREADS,TH);
#endif
	end = gethrtime();
	interval=end-start;
  	on_one_thread printf("METRICS:Boruvka_sort %f\n",interval/NANO);
#if PROFILING	
	on_one_thread printf("PROFILING RESULTS:\n");	
	printf("THREAD %d: compact_list_time %f s\n", MYTHREAD,compact_list_time[MYTHREAD]);
	printf("THREAD %d: compact_time %f s\n", MYTHREAD,compact_time[MYTHREAD]);
	printf("THREAD %d: sort_time %f s\n", MYTHREAD,sort_time[MYTHREAD]);
#endif
	

#if 0
	start=gethrtime();
	Boruvka_sparse_2(grph_list,n_vertices,TH);
        node_Barrier();
  	end = gethrtime();
  	interval=end-start;
  	on_one_thread printf("METRICS:Boruvka_sparse_2 time is %f\n",interval/NANO);

	start=gethrtime();
	Boruvka_sparse_3(grph_list,n_vertices,TH);
        node_Barrier();
  	end = gethrtime();
  	interval=end-start;
  	on_one_thread printf("METRICS:Boruvka_sparse_3 time is %f\n",interval/NANO);
#endif

  }
  node_Barrier();


  on_one_thread{
/*	delete_grph(G,n_vertices);*/
  } 

  node_Barrier();
  SIMPLE_done(TH);
}

