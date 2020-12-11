#include <sys/types.h>
#include "simple.h"
#include "../grph.h"
#include "values.h"

#define NANO 1000000000
ent_t* grph_list;
ent1_t * grph1_list;
int ** grph_matrix;
int d_or_s;
ent1_t * transform(ent_t * grph_list,int n_vertices);

void *SIMPLE_main(THREADED)
{
  int i,t,j, n_vertices,N,k,max=0;
  hrtime_t start,end;
  double interval,total=0;
  char * input_file;
  int * D;

  /*initialize graph from input file */  

  input_file = THARGV[0];
  if(MYTHREAD==0){
	t = read_graph(input_file,&n_vertices,&d_or_s,&grph_list,&grph_matrix);
	if(t!=0) exit(0);
  }
  n_vertices=node_Bcast_i(n_vertices,TH);
  node_Barrier();
  if(d_or_s==DENSE) {
	printf("dense\n");
  } else {

	int min_d=MAXINT,max_d=0;
	on_one printf("sparse\n");

	on_one_thread  grph1_list = transform(grph_list,n_vertices);
	node_Barrier();
	on_one_thread{
		for(i=0;i<n_vertices;i++){
			if(grph_list[i].n_neighbors>max_d) max_d=grph_list[i].n_neighbors;
			if(grph_list[i].n_neighbors<min_d) min_d=grph_list[i].n_neighbors;
			}
		printf("min_d is %d, max_d is %d\n", min_d,max_d);
	}

#if 1 
	init_mem(sizeof(ele_t)*(n_vertices*50),TH);
	start=gethrtime();
	Boruvka_sparse_nocompact(grph1_list,n_vertices,TH);	
 	node_Barrier();
  	end = gethrtime();
  	interval=end-start;
  	on_one_thread printf("METRICS:Boruvka_sparse_nocompact %f\n",interval/NANO);
	node_Barrier();
	clear_mem(TH);
#endif

 	
	
#if PROFILING	
	on_one_thread printf("PROFILING RESULTS:\n");	
	printf("THREAD %d: compact_list_time %f s\n", MYTHREAD,compact_list_time[MYTHREAD]);
	printf("THREAD %d: compact_time %f s\n", MYTHREAD,compact_time[MYTHREAD]);
	printf("THREAD %d: sort_time %f s\n", MYTHREAD,sort_time[MYTHREAD]);
#endif
	}	


  on_one_thread{
/*	delete_grph(G,n_vertices);*/
  } 

  node_Barrier();
  SIMPLE_done(TH);
}
