#include <sys/types.h>
#include "simple.h"
#include "../grph.h"
#include "values.h"

#define NANO 1000000000
ent_t* grph_list,*tmp;
int ** grph_matrix;
int d_or_s;

ele_t the_list[52]={{11,1},  {11,1},  {546162,1},  {43447,1},  {200119,1},  {567943,1},  {188711,1},  {11,1},  {11,1}, 
{79812,1},  {140986,1},  {606256,1},  {11,1},  {11,1},  {389809,1},  {459240,1},  {38806,1},  {157068,1},  {86322,1}, 
{764821,1},  {116840,1},  {641180,1},  {11,1},  {82293,1},  {280003,1},  {243050,1},  {530452,1},  {48921,1},  {47817,1}, 
{57187,1},  {584453,1},  {11,1},  {50509,1},  {338893,1},  {213725,1},  {368823,1},  {402317,1},  {11,1},  {11,1}, 
{98681,1},  {248144,1},  {358318,1},  {11,1},  {11,1},  {390621,1},  {416595,1},  {327827,1},  {135389,1},  {465232,1}, 
{52365,1},  {207766,1},  {107263,1}};

void *SIMPLE_main(THREADED)
{
  int i,t,j, n_vertices,N,k,max=0;
  hrtime_t start,end;
  double interval,total=0;
  char * input_file;
  int * D;
  ele_t *tmp_list;

  /*initialize graph from input file */  

  input_file = THARGV[0];

#if 0 

  tmp_list = malloc(sizeof(ele_t)*(52*2));
  for(i=0;i<52;i++)
	tmp_list[i]=the_list[i]; 
  mergesort_nr(the_list,52);
  check_sort_arr(the_list,0,51);	
  printf("The sorted the_list\n");
  for(i=0;i<52;i++)
  	printf("(%d,%d) ", the_list[i].v,the_list[i].w);
printf("\n");
  mergesort_nr(tmp_list,52);
  check_sort_arr(tmp_list,0,51);
  printf("The sorted tmp_list\n");
  for(i=0;i<52;i++)
  	printf("(%d,%d) ", the_list[i].v,the_list[i].w);
  printf("\n");
  return;

#endif
  	
  if(MYTHREAD==0){
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
	on_one printf("sparse\n");

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
	Boruvka_sparse_merge(grph_list,n_vertices,TH);	
 	node_Barrier();
  	end = gethrtime();
  	interval=end-start;
  	on_one_thread printf("METRICS:Boruvka_sparse_merge(my_alloc) %f\n",interval/NANO);
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
