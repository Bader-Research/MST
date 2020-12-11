#include <stdlib.h>
#include <stdio.h>
#include "../grph.h"
#include <string.h>

int read_graph(char * graph_name,int * n_vertices, int *d_or_s,ent_t** p_grph_list, int *** p_grph_matrix )
{
#define LINELEN 10000
  FILE * fp;
  int ** grph_matrix;
  ent_t * grph_list;
  char  description[LINELEN];
  char  pTemp[LINELEN];
  int n,i,j,w,*D;
 
  fp = fopen(graph_name,"rt");
  fscanf(fp,"%s",description);

  if(strstr(description,"dense")) {
    *d_or_s=DENSE;
    fscanf(fp,"%d",&n );
    grph_matrix =(int **) malloc (sizeof(int*)*n);
    for(i=0;i<n;i++)
       grph_matrix[i]=(int *)malloc(sizeof(int)*n);
    for(i=0;i<n;i++)
      for(j=0;j<n;j++)
      {
         fscanf(fp,"%d",&w);
	 grph_matrix[i][j]=w;
      }
  }
  else if(strstr(description,"sparse")){
    fscanf(fp,"%d",&n );
    *d_or_s=SPARSE;
    grph_list = (ent_t *)malloc(sizeof(ent_t)*n);
    D =(int*) malloc(sizeof(int)*n);
    fgets(pTemp,LINELEN,fp); //skip the new line symbol
    for(i=0;i<n;i++)
    {
	fgets(pTemp,LINELEN,fp);
        sscanf(pTemp,"%d",&D[i]);
    }
    for(i=0;i<n;i++)
    {
	grph_list[i].n_neighbors=D[i];
        grph_list[i].my_neighbors=(ele_t*)malloc(sizeof(ele_t)*D[i]);
    }
    rewind(fp);
    fgets(pTemp,LINELEN,fp); //skip the description line
    fgets(pTemp,LINELEN,fp); //skip the <n> line
    for(i=0;i<n;i++)
    {
      fscanf(fp,"%d",&w); // skip the degree section
      for(j=0;j<D[i];j++)
        fscanf(fp, "%d %d", &grph_list[i].my_neighbors[j].v, & grph_list[i].my_neighbors[j].w);
     } 
     free(D);
  } else {
    printf("First line should specify the representation of the graph\n");
    fclose(fp);
    return (-1);
  }

  fclose(fp);
  * p_grph_list=grph_list;
  * p_grph_matrix = grph_matrix;
  * n_vertices =n;
  return 0; 
}

int edge_number(ent_t *grph_list, int n_vertices)
{

  int n=0,i;

  for(i=0;i<n_vertices;i++)
	n+=grph_list[i].n_neighbors;

  return(n/2);
}

/*transform the representation of the sparse graph from ent_t * to ent1_t * which is used
 in Boruvka_sparse_nocompact*/
ent1_t * transform(ent_t * grph_list,int n_vertices)
{
	ent1_t * grph_list1;
	adj_t * p;
	int i,j;
	
	grph_list1 = malloc(sizeof(ent1_t)*n_vertices);
	for(i=0;i<n_vertices;i++)
	{
		grph_list1[i].n_neighbors=grph_list[i].n_neighbors;
		grph_list1[i].head = malloc(sizeof(adj_t));
		p=grph_list1[i].head;
		p->n_neighbors=grph_list[i].n_neighbors;
		p->my_neighbors = malloc(sizeof(ele_t)*(p->n_neighbors));
		for(j=0;j<grph_list[i].n_neighbors;j++)
			p->my_neighbors[j]=grph_list[i].my_neighbors[j];
		p->next=NULL;
		grph_list1[i].tail = p;
		grph_list1[i].wk_space1=i;
	}
	return (grph_list1);
}
