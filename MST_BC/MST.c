#include <sys/types.h>
#include "simple.h"
#include "../grph.h"
#include "values.h"
#include <sys/time.h>


ent_t* grph_list,*tmp;
int ** grph_matrix;
int d_or_s;

void *SIMPLE_main(THREADED)
{
  int i,t,j, n_vertices,N,k,max=0;
  hrtime_t start,end;
  double interval,total=0;
  char * input_file;
  int * D;

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
  } else {
	int min_d=MAXINT,max_d=0,m;
	on_one printf("sparse\n");

	on_one_thread{
		for(i=0;i<n_vertices;i++){
			if(grph_list[i].n_neighbors>max_d) max_d=grph_list[i].n_neighbors;
			if(grph_list[i].n_neighbors<min_d) min_d=grph_list[i].n_neighbors;
			}
		printf("min_d is %d, max_d is %d\n", min_d,max_d);
	}
	
	on_one_thread m=edge_number(grph_list,n_vertices);
	m = node_Bcast_i(m,TH);
	init_mem(sizeof(edge_t)*6*m+sizeof(int)*n_vertices*THREADS,TH);

	start = gethrtime();	
	Prim_Par(grph_list,n_vertices,m,TH);
	node_Barrier();
	end = gethrtime();	
	interval = end - start;
	on_one printf("METRICS:time used in Prim_par is %f s\n",interval/1000000000);	
 	clear_mem(TH); 
  }
  node_Barrier();


  on_one_thread{
/*	delete_grph(G,n_vertices);*/
  } 

  node_Barrier();
  SIMPLE_done(TH);
}






